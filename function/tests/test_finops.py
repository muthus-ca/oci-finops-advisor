import copy
import os
import tempfile
import unittest
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch
import oci
from analytics import Ledger, days, normalize, periods, query, reconcile, report_sections, shift_months
from func import Config, Deadline, Pipeline, YieldRun


def row(day='2026-09-01', value='10.10', currency='USD'):
    return dict(date=day, value=value, currency=currency, compartment_id='ocid.comp.a',
                compartment_path='/root/A', service='Compute', region='', sku_part_number='', unit='')


def chunk(costs):
    return {'views': {'cost': costs, 'region': copy.deepcopy(costs), 'total': copy.deepcopy(costs), 'usage': []}}


class AnalyticsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ledger = Ledger(self.tmp.name+'/test.sqlite')

    def tearDown(self):
        self.ledger.close(); self.tmp.cleanup()

    def test_calendar_boundaries(self):
        self.assertEqual(shift_months(date(2024, 2, 29), -12), date(2023, 2, 28))
        self.assertEqual(shift_months(date(2026, 3, 31), -1), date(2026, 2, 28))
        self.assertEqual(list(days(date(2026, 9, 1), date(2026, 9, 3))), ['2026-09-01', '2026-09-02'])

    def test_reconciliation_by_day_and_currency(self):
        data = chunk([row(value='0.1'), row(value='0.2')])['views']
        data['total'] = [row(value='0.3')]
        self.assertTrue(all(c['passed'] for c in reconcile(data)))
        data['region'][0]['value'] = '1.0'
        with self.assertRaisesRegex(ValueError, 'Reconciliation failed'): reconcile(data)

    def test_forecasts_excluded_and_missing_amount_rejected(self):
        item = {'time_usage_started': '2026-09-01T00:00:00Z', 'computed_amount': 12, 'currency': 'USD', 'is_forecast': True}
        self.assertEqual(normalize([item], 'cost', '2026-09-01', '2026-09-02'), [])
        item.update(is_forecast=False, computed_amount=None, attributed_cost='30')
        with self.assertRaisesRegex(ValueError, 'Missing computed_amount'): normalize([item], 'cost', '2026-09-01', '2026-09-02')

    def test_normalize_rejects_out_of_window_and_nan(self):
        item = {'time_usage_started': '2026-09-02', 'computed_amount': 5, 'currency': 'USD'}
        with self.assertRaises(ValueError): normalize([item], 'cost', '2026-09-01', '2026-09-02')
        item.update(time_usage_started='2026-09-01', computed_amount=float('nan'))
        with self.assertRaises(ValueError): normalize([item], 'cost', '2026-09-01', '2026-09-02')

    def test_exact_arithmetic_and_currency_partitioning(self):
        self.ledger.add(chunk([row(value='0.1'), row(value='0.2'), row(value='100', currency='INR')]), {'2026-09-01'})
        self.ledger.ready()
        result = query(self.ledger, {'start_date': '2026-09-01', 'end_date': '2026-09-02'})
        parts = {p['dimensions']['currency']: p for p in result['partitions']}
        self.assertEqual(parts['USD']['rows'][0]['value'], '0.3')
        self.assertEqual(parts['INR']['rows'][0]['value'], '100')

    def test_revision_replaces_day_not_adds(self):
        self.ledger.add(chunk([row(value='100'), row(day='2026-09-02', value='200')]), {'2026-09-01'})
        self.ledger.add(chunk([row(day='2026-09-02', value='250')]), {'2026-09-02'})
        self.ledger.ready()
        self.assertEqual(self.ledger.aggregate('cost', '2026-09-01', '2026-09-03', ['currency'])[0]['value'], '350')

    def test_missing_history_not_zero(self):
        self.ledger.add(chunk([row()]), {'2026-09-01'}); self.ledger.ready()
        self.assertEqual(query(self.ledger, {'start_date': '2026-08-31', 'end_date': '2026-09-02'})['status'], 'insufficient_history')
        sections = list(report_sections(self.ledger, {'id': 'test', 'start': '2026-08-31', 'end': '2026-09-02', 'previous_start': '2026-08-29'}, 'now'))
        self.assertEqual(len(sections), 1); self.assertIn('No partial total', sections[0][1])

    def test_empty_api_days_disclosed(self):
        self.ledger.add(chunk([]), {'2026-09-01'})
        self.assertEqual(self.ledger.coverage('2026-09-01', '2026-09-02')['days_with_no_cost_records'], ['2026-09-01'])

    def test_usage_never_mixes_units_or_skus(self):
        data = chunk([row()]); a = row(value='10'); b = row(value='1000')
        a.update(unit='OCPU HOURS', sku_part_number='B1'); b.update(unit='GB MONTHS', sku_part_number='B2')
        data['views']['usage'] = [a, b]; self.ledger.add(data, {'2026-09-01'}); self.ledger.ready()
        result = query(self.ledger, {'start_date': '2026-09-01', 'end_date': '2026-09-02', 'metric': 'usage', 'group_by': ['compartment_id']})
        self.assertEqual(len(result['partitions']), 2)

    def test_query_rejects_incompatible_dimensions_and_sql(self):
        for request in ({'group_by': ['service; DROP TABLE facts']}, {'group_by': ['region', 'compartment_path']}, {'metric': 'usage', 'filters': {'currency': 'USD'}}):
            with self.assertRaises(ValueError): query(self.ledger, {'start_date': '2026-09-01', 'end_date': '2026-09-02', **request})

    def test_two_year_periods_and_comparison(self):
        plan = periods(date(2024, 9, 29), date(2026, 9, 29))
        self.assertEqual(next(p for p in plan if p['id'] == 'last_12_months')['previous_start'], '2024-09-29')
        self.assertEqual(next(p for p in plan if p['id'] == 'last_24_months')['start'], '2024-09-29')


class MemoryStore:
    def __init__(self): self.items = {}; self.rev = 0; self.fail_key = None
    def get(self, key, optional=False):
        if key not in self.items:
            if optional: return None, None
            raise KeyError(key)
        payload, rev = self.items[key]; return copy.deepcopy(payload), str(rev)
    def put(self, key, value, **conditions):
        if self.fail_key and self.fail_key in key:
            self.fail_key = None; raise RuntimeError('simulated upload interruption')
        if conditions.get('if_none_match') == '*' and key in self.items: raise RuntimeError('CAS conflict')
        if 'if_match' in conditions and (key not in self.items or str(self.items[key][1]) != conditions['if_match']): raise RuntimeError('CAS conflict')
        self.rev += 1; self.items[key] = (copy.deepcopy(value), self.rev); return str(self.rev)
    def put_bytes(self, key, payload, content_type, **conditions): return self.put(key, payload.decode(), **conditions)
    def delete(self, key, **conditions): self.items.pop(key, None)
    @contextmanager
    def lock(self): yield


class FakeUsage:
    def __init__(self): self.calls = []; self.fail_on = None
    def request_summarized_usages(self, details, **kwargs):
        self.calls.append((details.group_by, kwargs.get('page')))
        if self.fail_on == len(self.calls): raise RuntimeError('simulated API interruption')
        amount = 0.2 if kwargs.get('page') else 0.1
        items = [oci.usage_api.models.UsageSummary(time_usage_started=datetime.fromisoformat(day).replace(tzinfo=timezone.utc),
                    computed_amount=amount, computed_quantity=amount, currency='USD', compartment_id='ocid.comp.a',
                    compartment_path='/root/A', service='Compute', region='us-phoenix-1', unit='OCPU HOURS', sku_part_number='B1')
                 for day in days(details.time_usage_started.date(), details.time_usage_ended.date())]
        return SimpleNamespace(data=oci.usage_api.models.UsageAggregation(items=items), headers={} if kwargs.get('page') else {'opc-next-page': 'next'})


class FakeAgent:
    def __init__(self): self.jobs = []; self.creates = 0; self.phase = 'SUCCEEDED'; self.failed_files = 0; self.fail_create_response = False
    def list_data_ingestion_jobs(self, **kwargs):
        return SimpleNamespace(data=SimpleNamespace(items=[j for j in self.jobs if j.display_name == kwargs['display_name']]), headers={})
    def create_data_ingestion_job(self, details, **kwargs):
        self.creates += 1; job = SimpleNamespace(id='job-'+str(self.creates), display_name=details.display_name); self.jobs.append(job)
        if self.fail_create_response: self.fail_create_response = False; raise RuntimeError('response lost after job accepted')
        return SimpleNamespace(data=job, headers={})
    def get_data_ingestion_job(self, job_id):
        return SimpleNamespace(data=SimpleNamespace(lifecycle_state=self.phase, lifecycle_details='test',
              data_ingestion_job_statistics=oci.generative_ai_agent.models.DataIngestionJobStatistics(number_of_failed_files=self.failed_files)), headers={})


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {'TENANCY_OCID': 'ocid.tenancy', 'NAMESPACE': 'ns', 'BUCKET_NAME': 'bucket', 'RUN_INGESTION': 'false', 'HISTORY_YEARS': '2'}, clear=True); self.env.start()
        self.cfg = Config(); self.store, self.usage, self.agent = MemoryStore(), FakeUsage(), FakeAgent()
        self.clock = patch('func.utcnow', return_value=datetime(2026, 9, 5, tzinfo=timezone.utc)); self.clock.start()
        self.pipeline = self.make(); self.pipeline.load(); self.pipeline.start()
        self.pipeline.state['history_start'] = '2026-09-01'; self.pipeline.state['cycle']['cursor'] = '2026-09-01'; self.pipeline.save()
    def tearDown(self): self.clock.stop(); self.env.stop()
    def make(self): return Pipeline(self.cfg, self.store, self.usage, self.agent, Deadline(3000, 150))
    def test_page_resume_and_no_duplicate_totals(self):
        self.usage.fail_on = 2
        with self.assertRaisesRegex(RuntimeError, 'API interruption'): self.pipeline.collect()
        other = self.make(); other.load(); other.collect()
        self.assertEqual(self.usage.calls[2][1], 'next'); self.assertEqual(other.state['cycle']['phase'], 'reports')
        with other.ledger(other.state['days']) as ledger: self.assertEqual(Decimal(ledger.aggregate('cost', '2026-09-01', '2026-09-03', ['currency'])[0]['value']), Decimal('0.6'))
    def test_full_publish_then_refresh_catches_missed_weeks(self):
        self.pipeline.collect(); self.pipeline.reports(); self.pipeline.publish()
        self.assertEqual(self.pipeline.state['published_end'], '2026-09-03'); self.assertTrue(self.pipeline.state['published_objects'])
        with patch('func.utcnow', return_value=datetime(2026, 10, 20, tzinfo=timezone.utc)): self.pipeline.start()
        self.assertEqual(self.pipeline.state['cycle']['target'], '2026-10-18'); self.assertEqual(self.pipeline.state['cycle']['cursor'], '2026-09-01')
    def test_report_interruption_does_not_publish_or_ingest(self):
        self.pipeline.collect(); self.store.fail_key = '.metadata.json'
        with self.assertRaises(RuntimeError): self.pipeline.reports()
        self.assertIsNone(self.pipeline.state['published_end'])
        other = self.make(); other.load(); other.reports(); other.publish()
        self.assertIsNotNone(other.state['published_end']); self.assertEqual(self.agent.creates, 0)
    def test_ingestion_recovers_lost_create_response(self):
        self.pipeline.collect(); self.pipeline.reports(); self.pipeline.publish()
        self.cfg.ingest = True; self.cfg.source = 'source'; self.cfg.compartment = 'comp'; self.agent.fail_create_response = True
        with self.assertRaises(RuntimeError): self.pipeline.ingestion()
        other = self.make(); other.load(); self.assertEqual(other.ingestion()['status'], 'ingestion_submitted')
        self.assertEqual(self.agent.creates, 1); self.assertIsNone(other.ingestion()); self.assertEqual(other.state['ingestion']['status'], 'SUCCEEDED')
    def test_partial_ingestion_is_failure_and_retry_is_explicit(self):
        self.pipeline.collect(); self.pipeline.reports(); self.pipeline.publish()
        self.cfg.ingest = True; self.cfg.source = 'source'; self.cfg.compartment = 'comp'; self.pipeline.ingestion(); self.agent.failed_files = 2
        self.assertEqual(self.pipeline.ingestion()['status'], 'ingestion_failed'); self.assertEqual(self.pipeline.ingestion()['status'], 'ingestion_failed')
        self.assertEqual(self.agent.creates, 1); self.pipeline.ingestion(retry_failed=True); self.assertEqual(self.agent.creates, 2)
    def test_old_published_manifest_is_used_during_refresh(self):
        self.pipeline.collect(); self.pipeline.reports(); self.pipeline.publish(); published = copy.deepcopy(self.pipeline.state['published_days'])
        self.pipeline.start(force=True); self.pipeline.collect()
        self.assertEqual(self.pipeline.state['published_days'], published); self.assertNotEqual(self.pipeline.state['days'], published)
    def test_configuration_change_rejected(self):
        self.cfg.fingerprint = 'changed'
        with self.assertRaisesRegex(ValueError, 'configuration changed'): self.make().load()
    def test_deadline_yields(self):
        with self.assertRaises(YieldRun): Deadline(0, 0).check()


if __name__ == '__main__': unittest.main()

class HistoricalPathTests(unittest.TestCase):
    def test_renamed_compartment_not_duplicated_or_lost(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = Ledger(directory+'/data.sqlite')
            a = row(value='10'); b = row(value='20'); b['compartment_path'] = '/root/Renamed'
            ledger.add(chunk([a, b]), {'2026-09-01'}); ledger.ready()
            try:
                sections = list(report_sections(ledger, {'id': 'test', 'start': '2026-09-01', 'end': '2026-09-02', 'previous_start': '2026-08-31'}, 'now'))
                names = [name for name, text in sections]
                self.assertEqual(len(names), len(set(names)))
                compartment = next(text for name, text in sections if '_comp_' in name)
                self.assertIn('30.00', compartment)
                self.assertIn('/root/Renamed', compartment)
                ranking = next(text for name, text in sections if name == 'USD_Compartments_0')
                self.assertIn('30.00', ranking)
            finally: ledger.close()
