"""OCI Functions entrypoint: resumable cost archive, reports, and ingestion."""
import gzip
import hashlib
import io
import json
import logging
import os
import re
import tempfile
import time
import uuid
from collections import defaultdict
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import oci
from fdk import response

from analytics import VIEWS, Ledger, days, normalize, periods, query, reconcile, report_sections, shift_months

LOG = logging.getLogger('finops')
LOG.setLevel(logging.INFO)
LOG.propagate = False
if not LOG.handlers:
    LOG.addHandler(logging.StreamHandler())
SCHEMA_VERSION = 1


class YieldRun(Exception):
    pass


class Busy(Exception):
    pass


def log(event, **values):
    LOG.info(json.dumps({'event': event, **values}, default=str))


def utcnow():
    return datetime.now(timezone.utc)


def required(name):
    value = os.environ.get(name, '').strip()
    if not value:
        raise ValueError('Missing required configuration: ' + name)
    return value


def integer(name, default, low, high):
    value = int(os.environ.get(name, str(default)))
    if not low <= value <= high:
        raise ValueError('%s must be %s..%s' % (name, low, high))
    return value


def boolean(name, default):
    value = os.environ.get(name, str(default)).lower()
    if value not in ('true', 'false'):
        raise ValueError(name + ' must be true or false')
    return value == 'true'


class Config:
    def __init__(self):
        self.tenancy = required('TENANCY_OCID')
        self.bucket = required('BUCKET_NAME')
        if not self.bucket:
            raise ValueError('BUCKET_NAME cannot be empty')
        self.namespace = required('NAMESPACE')
        if not self.namespace:
            raise ValueError('NAMESPACE cannot be empty')
        self.region = required('OCI_REGION')
        self.agent_region = os.environ.get('GENAI_REGION', self.region)
        self.raw = os.environ.get('RAW_PREFIX', 'cost-analysis/raw').strip('/')
        self.curated = os.environ.get('CURATED_PREFIX', 'cost-analysis/curated').strip('/')
        self.state = os.environ.get('STATE_PREFIX', 'cost-analysis/state').strip('/')
        prefixes = (self.raw, self.curated, self.state)
        if any(not p for p in prefixes) or any(a == b or a.startswith(b+'/')
                                                for i, a in enumerate(prefixes) for j, b in enumerate(prefixes) if i != j):
            raise ValueError('RAW_PREFIX, CURATED_PREFIX, STATE_PREFIX must be separate nonempty prefixes')
        self.years = integer('HISTORY_YEARS', 1, 1, 2)
        self.chunk_days = integer('CHUNK_DAYS', 7, 1, 31)
        self.refresh_days = integer('REVISION_LOOKBACK_DAYS', 35, 1, 365)
        self.lag = integer('DATA_LAG_DAYS', 2, 1, 7)
        self.depth = integer('COMPARTMENT_DEPTH', 6, 1, 6)
        self.usage = boolean('INCLUDE_USAGE', True)
        self.ingest = boolean('RUN_INGESTION', False)
        self.compartment = required('COMPARTMENT_ID') if self.ingest else os.environ.get('COMPARTMENT_ID')
        self.source = required('DATA_SOURCE_ID') if self.ingest else os.environ.get('DATA_SOURCE_ID')
        self.read_timeout = integer('API_READ_TIMEOUT_SECONDS', 120, 30, 240)
        self.runtime = integer('MAX_RUNTIME_SECONDS', 3000, 300, 3300)
        self.max_rows = integer('MAX_WINDOW_ROWS', 200000, 1000, 2000000)
        self.max_object = integer('MAX_OBJECT_MB', 128, 1, 512) * 1024 * 1024
        self.views = {k: v for k, v in VIEWS.items() if k != 'usage' or self.usage}
        self.fingerprint = hashlib.sha256(json.dumps({
            'schema': SCHEMA_VERSION, 'report_version': 2, 'tenancy': self.tenancy, 'region': self.region,
            'namespace': self.namespace, 'bucket': self.bucket, 'raw': self.raw,
            'curated': self.curated, 'views': self.views, 'depth': self.depth,
            'years': self.years,
        }, sort_keys=True).encode()).hexdigest()


class Deadline:
    def __init__(self, seconds, reserve):
        self.end = time.monotonic() + seconds
        self.reserve = reserve

    def check(self):
        if time.monotonic() + self.reserve >= self.end:
            raise YieldRun('Execution budget reached; invoke again to resume')

    def call(self, stage, fn, *args, expected_statuses=(), **kwargs):
        self.check()
        started = time.monotonic()
        log('started', stage=stage)
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:
            if isinstance(exc, oci.exceptions.ServiceError) and exc.status in expected_statuses:
                log('expected_response', stage=stage, http_status=exc.status)
            else:
                LOG.exception('Stage failed: %s after %.2fs', stage, time.monotonic()-started)
            raise
        log('completed', stage=stage, seconds=round(time.monotonic()-started, 3),
            opc_request_id=getattr(result, 'headers', {}).get('opc-request-id'))
        return result


class Store:
    def __init__(self, client, cfg, budget):
        self.client, self.cfg, self.budget = client, cfg, budget

    def get(self, key, optional=False):
        try:
            result = self.budget.call('get:'+key, self.client.get_object,
                                      self.cfg.namespace, self.cfg.bucket, key,
                                      expected_statuses=(404,) if optional else ())
        except oci.exceptions.ServiceError as exc:
            if optional and exc.status == 404:
                return None, None
            raise
        try:
            # Explicit bound also protects against accidentally reading the wrong object.
            payload = result.data.raw.read(self.cfg.max_object + 1, decode_content=False)
            if len(payload) > self.cfg.max_object:
                raise ValueError('Object exceeds MAX_OBJECT_MB: '+key)
            if key.endswith('.gz'):
                with gzip.GzipFile(fileobj=io.BytesIO(payload)) as reader:
                    payload = reader.read(self.cfg.max_object+1)
                if len(payload) > self.cfg.max_object:
                    raise ValueError('Expanded object exceeds MAX_OBJECT_MB: '+key)
            return json.loads(payload), result.headers.get('etag')
        finally:
            result.data.close()

    def put(self, key, value, **conditions):
        payload = json.dumps(value, separators=(',', ':'), default=str).encode()
        if len(payload) > self.cfg.max_object:
            raise ValueError('JSON exceeds MAX_OBJECT_MB; reduce CHUNK_DAYS')
        if key.endswith('.gz'):
            payload = gzip.compress(payload, mtime=0)
        return self.put_bytes(key, payload, 'application/gzip' if key.endswith('.gz') else 'application/json', **conditions)

    def put_bytes(self, key, payload, content_type, **conditions):
        result = self.budget.call('put:'+key, self.client.put_object,
                                  self.cfg.namespace, self.cfg.bucket, key, payload,
                                  content_type=content_type, **conditions)
        return result.headers.get('etag')

    def delete(self, key, **conditions):
        try:
            self.budget.call('delete:'+key, self.client.delete_object,
                             self.cfg.namespace, self.cfg.bucket, key, **conditions)
        except oci.exceptions.ServiceError as exc:
            if exc.status != 404:
                raise

    @contextmanager
    def lock(self):
        key = self.cfg.state+'/lock.json'
        owner = str(uuid.uuid4())
        body = {'owner': owner, 'expires': time.time()+3720}
        existing, etag = self.get(key, optional=True)
        if existing and existing['expires'] > time.time():
            raise Busy('Another run holds the lock; retry after it finishes')
        condition = {'if_match': etag} if existing else {'if_none_match': '*'}
        try:
            acquired = self.put(key, body, **condition)
        except oci.exceptions.ServiceError as exc:
            if exc.status == 412:
                raise Busy('Another invocation acquired the lock') from exc
            raise
        try:
            yield
        finally:
            # Release is allowed in the reserved budget; conditional deletion never removes another owner.
            try:
                self.client.delete_object(self.cfg.namespace, self.cfg.bucket, key, if_match=acquired)
            except Exception:
                LOG.exception('Lock release failed; lease expires automatically')


class Pipeline:
    def __init__(self, cfg, store, usage_client, agent_client, budget):
        self.cfg, self.store = cfg, store
        self.usage_client, self.agent_client, self.budget = usage_client, agent_client, budget
        self.key = cfg.state+'/checkpoint.json'
        self.state, self.etag = None, None

    def load(self):
        self.state, self.etag = self.store.get(self.key, optional=True)
        if self.state and self.state['fingerprint'] != self.cfg.fingerprint:
            raise ValueError('Collection configuration changed. Use a fresh set of prefixes/bucket or restore it; do not mix ledgers.')
        return self.state

    def save(self):
        conditions = {'if_match': self.etag} if self.etag else {'if_none_match': '*'}
        self.etag = self.store.put(self.key, self.state, **conditions)

    def status(self):
        s = self.state
        if not s:
            return {'status': 'not_initialized'}
        cycle = s.get('cycle')
        return {'status': 'in_progress' if cycle else 'collection_complete',
                'history_start': s['history_start'], 'published_end_exclusive': s.get('published_end'),
                'queried_days': len(s['days']), 'phase': cycle['phase'] if cycle else None,
                'next_date': cycle.get('cursor') if cycle else None,
                'ingestion': s.get('ingestion'),
                'next_action': 'Invoke refresh again until collection_complete; then use weekly scheduling.' if cycle else None}

    def start(self, force=False):
        end = utcnow().date()-timedelta(days=self.cfg.lag)
        if self.state is None:
            start = shift_months(end, -12*self.cfg.years)
            self.state = {'schema_version': SCHEMA_VERSION, 'fingerprint': self.cfg.fingerprint,
                          'history_start': start.isoformat(), 'days': {}, 'published_days': {},
                          'published_objects': [], 'published_end': None, 'cycle': None,
                          'ingestion': None}
        if self.state['cycle']:
            return
        published = self.state['published_end']
        if published and end.isoformat() <= published and not force:
            return
        start = date.fromisoformat(self.state['history_start'])
        if published:
            start = max(start, date.fromisoformat(published)-timedelta(days=self.cfg.refresh_days))
        self.state['cycle'] = {'id': str(uuid.uuid4()), 'generated': utcnow().isoformat(),
                               'target': end.isoformat(), 'cursor': start.isoformat(),
                               'phase': 'collect', 'window': None, 'period_index': 0,
                               'uploaded': [], 'cleanup_index': 0}
        self.save()

    def collect(self):
        c = self.state['cycle']
        views = list(self.cfg.views)
        while c['cursor'] < c['target']:
            self.budget.check()
            if not c['window']:
                end = min(date.fromisoformat(c['cursor'])+timedelta(days=self.cfg.chunk_days),
                          date.fromisoformat(c['target']))
                c['window'] = {'start': c['cursor'], 'end': end.isoformat(),
                               'view_index': 0, 'page': None, 'page_number': 0, 'pages': []}
                self.save()
            w = c['window']
            while w['view_index'] < len(views):
                view = views[w['view_index']]
                kind, groups = self.cfg.views[view]
                details = oci.usage_api.models.RequestSummarizedUsagesDetails(
                    tenant_id=self.cfg.tenancy,
                    time_usage_started=datetime.fromisoformat(w['start']).replace(tzinfo=timezone.utc),
                    time_usage_ended=datetime.fromisoformat(w['end']).replace(tzinfo=timezone.utc),
                    granularity='DAILY', query_type=kind, group_by=groups,
                    compartment_depth=self.cfg.depth, is_aggregate_by_time=False)
                kwargs = {'limit': 1000}
                if w['page']:
                    kwargs['page'] = w['page']
                result = self.budget.call('usage:'+view+':'+w['start'],
                                          self.usage_client.request_summarized_usages, details, **kwargs)
                payload = oci.util.to_dict(result.data)
                rows = normalize(payload.get('items', []), view, w['start'], w['end'])
                page_key = (f'{self.cfg.raw}/api-pages/{c["id"]}/{w["start"]}/'
                            f'{view}/{w["page_number"]:06d}.json.gz')
                self.store.put(page_key, {'view': view, 'query_type': kind, 'group_by': groups,
                                          'start': w['start'], 'end': w['end'],
                                          'opc_request_id': result.headers.get('opc-request-id'),
                                          'fetched_utc': utcnow().isoformat(),
                                          'response': payload, 'rows': rows})
                w['pages'].append(page_key)
                nxt = result.headers.get('opc-next-page')
                if nxt and nxt == w['page']:
                    raise ValueError('Usage API repeated the pagination token')
                w['page'] = nxt
                w['page_number'] += 1
                if not nxt:
                    w['view_index'] += 1
                    w['page_number'] = 0
                self.save()
            combined = {v: [] for v in views}
            row_count = 0
            for page_key in w['pages']:
                page, _ = self.store.get(page_key)
                row_count += len(page['rows'])
                if row_count > self.cfg.max_rows:
                    raise ValueError('Window exceeds MAX_WINDOW_ROWS; see recovery instructions')
                combined[page['view']].extend(page['rows'])
            checks = reconcile(combined)
            chunk_key = f'{self.cfg.raw}/snapshots/{c["id"]}/{w["start"]}_{w["end"]}.json.gz'
            self.store.put(chunk_key, {'start': w['start'], 'end': w['end'],
                                      'fetched_utc': utcnow().isoformat(), 'views': combined,
                                      'reconciliation': checks})
            for day in days(date.fromisoformat(w['start']), date.fromisoformat(w['end'])):
                self.state['days'][day] = chunk_key
            c['cursor'] = w['end']
            c['window'] = None
            self.save()
        c['phase'] = 'reports'
        self.save()

    @contextmanager
    def ledger(self, manifest, start=None, end=None):
        with tempfile.TemporaryDirectory(prefix='finops-') as directory:
            ledger = Ledger(str(Path(directory)/'ledger.sqlite'))
            try:
                chunks = defaultdict(set)
                for day, key in manifest.items():
                    if (start is None or day >= start) and (end is None or day < end):
                        chunks[key].add(day)
                for key, selected_days in sorted(chunks.items()):
                    chunk, _ = self.store.get(key)
                    ledger.add(chunk, selected_days)
                ledger.ready()
                yield ledger
            finally:
                ledger.close()

    def metadata_schema(self):
        key = '_metadata_schema.json'
        schema, etag = self.store.get(key, optional=True)
        schema = schema or {'metadataSchema': []}
        fields = {entry['name']: entry['type'] for entry in schema['metadataSchema']}
        for name in ('finops_period', 'finops_start', 'finops_end', 'finops_generated'):
            if name in fields and fields[name] != 'string':
                raise ValueError('Conflicting metadata schema: '+name)
            if name not in fields:
                schema['metadataSchema'].append({'name': name, 'type': 'string'})
        self.store.put(key, schema, **({'if_match': etag} if etag else {'if_none_match': '*'}))

    def upload_report(self, name, text, period):
        c = self.state['cycle']
        key = self.cfg.curated+'/'+name+'.md'
        if key in c['uploaded']:
            return
        payload = text.encode('utf-8')
        if len(payload) > 5*1024*1024:
            raise ValueError('Report exceeds 5 MB; reduce report section size')
        self.store.put_bytes(key, payload, 'text/markdown; charset=utf-8')
        self.store.put(key+'.metadata.json', {'metadataAttributes': {
            'finops_period': period['id'], 'finops_start': period['start'],
            'finops_end': period['end'], 'finops_generated': c['generated']}})
        c['uploaded'].append(key)
        self.save()

    def reports(self):
        c = self.state['cycle']
        plan = periods(date.fromisoformat(self.state['history_start']), date.fromisoformat(c['target']))
        self.metadata_schema()
        with self.ledger(self.state['days']) as ledger:
            while c['period_index'] < len(plan):
                period = plan[c['period_index']]
                for name, text in report_sections(ledger, period, c['generated'], self.cfg.usage):
                    digest = hashlib.sha256(name.encode()).hexdigest()[:12]
                    readable = re.sub(r'[^a-z0-9_-]+', '-', name.lower()).strip('-')[:90]
                    self.upload_report('reports/'+period['id']+'/'+readable+'-'+digest, text, period)
                c['period_index'] += 1
                self.save()
        full = {'id': 'catalog', 'start': self.state['history_start'], 'end': c['target']}
        catalog = ('# OCI FinOps report catalog\n\nGenerated UTC: '+c['generated']+
                   '\nData cutoff (exclusive UTC): '+c['target']+
                   '\nHistorical collection start: '+self.state['history_start']+
                   '\nAll amounts are actual COST query amounts, not invoice payable totals. '
                   f'Configured compartment depth: {self.cfg.depth}. Match this setting in Cost Analysis. '
                   'Daily windows are queried and reconciled against separate total queries. '
                   'Empty historical windows are disclosed, not certified as zero.\n\n'+
                   '\n'.join('- '+p['id']+': '+p['start']+' to '+p['end']+' exclusive' for p in plan)+
                   '\n\nLast month means a trailing calendar-month interval; calendar_YYYY_MM means a named month. '
                   'Last year must be clarified as trailing 12 months or previous calendar year. '
                   'Reports overlap; do not add totals from different reports. '
                   'Custom queries require the query action/tool; do not reconstruct totals from partial retrieval.\n')
        self.upload_report('catalog', catalog, full)
        self.upload_report('optimization_playbook', Path(__file__).with_name('optimization_playbook.txt').read_text(), full)
        c['cleanup'] = sorted(set(self.state['published_objects'])-set(c['uploaded']))
        c['phase'] = 'publish'
        self.save()

    def publish(self):
        c = self.state['cycle']
        while c['cleanup_index'] < len(c['cleanup']):
            key = c['cleanup'][c['cleanup_index']]
            self.store.delete(key)
            self.store.delete(key+'.metadata.json')
            c['cleanup_index'] += 1
            self.save()
        self.state['published_days'] = dict(self.state['days'])
        self.state['published_objects'] = c['uploaded']
        self.state['published_end'] = c['target']
        self.state['published_generated'] = c['generated']
        self.state['ingestion'] = {'display_name': 'finops-'+c['id'],
                                   'retry_token': str(uuid.uuid4()), 'job_id': None,
                                   'status': 'PENDING', 'data_source_id': self.cfg.source}
        self.state['cycle'] = None
        self.save()

    def ingestion(self, retry_failed=False):
        job = self.state.get('ingestion')
        if not self.cfg.ingest or not job:
            return None
        if job['status'] == 'SUCCEEDED':
            return None
        if job.get('data_source_id') not in (None, self.cfg.source):
            raise ValueError('DATA_SOURCE_ID changed with pending ingestion; restore it and finish ingestion, or initialize a separate dataset')
        if retry_failed and job['status'] in ('FAILED', 'DELETED', 'PARTIAL_FAILURE'):
            job.update(display_name='finops-retry-'+str(uuid.uuid4()), retry_token=str(uuid.uuid4()),
                       job_id=None, status='PENDING')
            self.save()
        if job['status'] in ('FAILED', 'DELETED', 'PARTIAL_FAILURE'):
            return {'status': 'ingestion_failed', 'ingestion': job,
                    'next_action': 'Inspect ingestion logs, fix the cause, then invoke action=retry_ingestion.'}
        if not job['job_id']:
            # Recover an accepted create whose response was lost, including after token expiry.
            matches, page = [], None
            while True:
                kwargs = {'compartment_id': self.cfg.compartment, 'data_source_id': self.cfg.source,
                          'display_name': job['display_name'], 'limit': 100}
                if page:
                    kwargs['page'] = page
                result = self.budget.call('list_ingestion', self.agent_client.list_data_ingestion_jobs, **kwargs)
                matches.extend(result.data.items)
                page = result.headers.get('opc-next-page')
                if not page:
                    break
            if len(matches) > 1:
                raise ValueError('Multiple matching ingestion jobs; inspect before retrying')
            if matches:
                job['job_id'] = matches[0].id
            else:
                result = self.budget.call('create_ingestion', self.agent_client.create_data_ingestion_job,
                    oci.generative_ai_agent.models.CreateDataIngestionJobDetails(
                        compartment_id=self.cfg.compartment, data_source_id=self.cfg.source,
                        display_name=job['display_name'], description='Reconciled FinOps reports through '+self.state['published_end']),
                    opc_retry_token=job['retry_token'])
                job['job_id'] = result.data.id
            job['status'] = 'SUBMITTED'
            job['data_source_id'] = self.cfg.source
            self.save()
            return {'status': 'ingestion_submitted', 'ingestion': job}
        result = self.budget.call('get_ingestion', self.agent_client.get_data_ingestion_job, job['job_id'])
        job['status'] = result.data.lifecycle_state
        job['details'] = result.data.lifecycle_details
        job['statistics'] = oci.util.to_dict(result.data.data_ingestion_job_statistics)
        # Some jobs finish with file-level failures; do not call them fully successful.
        stats = job['statistics'] or {}
        if job['status'] == 'SUCCEEDED' and any(
                isinstance(v, (int, float)) and v > 0 for k, v in stats.items() if 'fail' in k.lower()):
            job['status'] = 'PARTIAL_FAILURE'
        self.save()
        if job['status'] == 'SUCCEEDED':
            return None
        return {'status': 'ingestion_failed' if job['status'] in ('FAILED', 'DELETED', 'PARTIAL_FAILURE')
                else 'ingestion_in_progress', 'ingestion': job}

    def run(self, request):
        action = request.get('action', 'refresh')
        self.load()
        if action == 'status':
            return self.status()
        if action == 'query':
            if not self.state or not self.state['published_days']:
                return {'status': 'not_ready', 'message': 'Finish the first collection/publication.'}
            if request.get('metric') == 'usage' and not self.cfg.usage:
                raise ValueError('Usage collection is disabled')
            with self.ledger(self.state['published_days'], request['start_date'], request['end_date']) as ledger:
                answer = query(ledger, request)
            answer['compartment_depth'] = self.cfg.depth
            answer['published_generated_utc'] = self.state.get('published_generated')
            answer['data_cutoff_exclusive'] = self.state['published_end']
            if len(json.dumps(answer).encode()) > 4*1024*1024:
                raise ValueError('Query result exceeds 4 MB; narrow filters or limit')
            return answer
        if action not in ('refresh', 'retry_ingestion', 'restart_window'):
            raise ValueError('action must be refresh, status, query, retry_ingestion, or restart_window')
        with self.store.lock():
            self.load()  # Re-read after acquiring the lock.
            if action == 'restart_window':
                if not self.state or not self.state.get('cycle') or self.state['cycle']['phase'] != 'collect':
                    raise ValueError('restart_window requires an active collection cycle')
                self.state['cycle']['window'] = None
                self.save()
                return self.status()
            if self.state and not self.state['cycle']:
                result = self.ingestion(retry_failed=action == 'retry_ingestion')
                if result:
                    return result
            if action == 'retry_ingestion':
                return self.status()
            self.start(force=request.get('force_refresh') is True)
            if self.state['cycle']:
                if self.state['cycle']['phase'] == 'collect':
                    self.collect()
                if self.state['cycle']['phase'] == 'reports':
                    self.reports()
                if self.state['cycle']['phase'] == 'publish':
                    self.publish()
                result = self.ingestion()
                if result:
                    return result
            return self.status()


def handler(ctx, data: io.BytesIO = None):
    pipeline = None
    try:
        raw = data.read(65537) if data else b''
        if len(raw) > 65536:
            raise ValueError('Request exceeds 64 KB')
        request = json.loads(raw) if raw.strip() else {}
        if not isinstance(request, dict):
            raise ValueError('Request must be a JSON object')
        cfg = Config()
        runtime = cfg.runtime
        if ctx is not None:
            # Respect the platform's actual deadline, including synchronous test invocations.
            platform_end = datetime.fromisoformat(ctx.Deadline().replace('Z', '+00:00'))
            runtime = min(runtime, max(0, (platform_end-utcnow()).total_seconds()-15))
        budget = Deadline(runtime, cfg.read_timeout+30)
        signer = oci.auth.signers.get_resource_principals_signer()
        kwargs = {'signer': signer, 'timeout': (10, cfg.read_timeout),
                  'retry_strategy': oci.retry.NoneRetryStrategy()}
        storage = oci.object_storage.ObjectStorageClient({'region': cfg.region}, **kwargs)
        usage = oci.usage_api.UsageapiClient({'region': cfg.region}, **kwargs)
        agent = oci.generative_ai_agent.GenerativeAiAgentClient({'region': cfg.agent_region}, **kwargs) if cfg.ingest else None
        store = Store(storage, cfg, budget)
        pipeline = Pipeline(cfg, store, usage, agent, budget)
        result = pipeline.run(request)
    except Busy as exc:
        result = {'status': 'busy', 'message': str(exc)}
    except YieldRun as exc:
        # Durable state is saved at each page/report boundary. Never mark this complete.
        result = {'status': 'resume_required', 'message': str(exc),
                  'next_action': 'Invoke refresh again. Continuation is not automatic.'}
    except Exception:
        LOG.exception('FinOps invocation failed; inspect the last stage and traceback')
        raise  # Preserve a failed invocation for OCI monitoring; never return success on errors.
    log('invocation_result', **result if result.get('status') != 'success' else {'status': 'success'})
    return response.Response(ctx, response_data=json.dumps(result, default=str),
                             headers={'Content-Type': 'application/json'})
