import gzip
import io
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import oci
from func import Busy, Config, Deadline, Store


class Raw(io.BytesIO):
    def read(self, amount=-1, decode_content=False): return super().read(amount)


class FakeObjectClient:
    def __init__(self): self.items = {}; self.counter = 0
    def error(self, status): raise oci.exceptions.ServiceError(status, 'test', {}, 'test')
    def get_object(self, ns, bucket, name):
        if name not in self.items: self.error(404)
        data, etag = self.items[name]
        return SimpleNamespace(data=SimpleNamespace(raw=Raw(data), close=lambda: None), headers={'etag': etag})
    def put_object(self, ns, bucket, name, data, **kwargs):
        self.check(name, kwargs)
        self.counter += 1; etag = str(self.counter); self.items[name] = (data, etag)
        return SimpleNamespace(headers={'etag': etag})
    def delete_object(self, ns, bucket, name, **kwargs):
        if name not in self.items: self.error(404)
        self.check(name, kwargs); del self.items[name]
    def check(self, name, kwargs):
        if kwargs.get('if_none_match') == '*' and name in self.items: self.error(412)
        if kwargs.get('if_match') and (name not in self.items or self.items[name][1] != kwargs['if_match']): self.error(412)


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {'TENANCY_OCID': 't', 'NAMESPACE': 'n', 'BUCKET_NAME': 'b'}, clear=True); self.env.start()
        self.cfg = Config(); self.client = FakeObjectClient(); self.store = Store(self.client, self.cfg, Deadline(3000, 150))
    def tearDown(self): self.env.stop()
    def test_gzip_roundtrip(self):
        self.store.put('a.json.gz', {'x': [1, '0.3']})
        self.assertEqual(self.store.get('a.json.gz')[0], {'x': [1, '0.3']})
    def test_lock_excludes_other_worker_and_releases(self):
        with self.store.lock():
            with self.assertRaises(Busy):
                with self.store.lock(): pass
        self.assertNotIn(self.cfg.state+'/lock.json', self.client.items)
    def test_expired_lock_is_replaced(self):
        self.store.put(self.cfg.state+'/lock.json', {'owner': 'old', 'expires': 0})
        with self.store.lock(): self.assertNotEqual(self.store.get(self.cfg.state+'/lock.json')[0]['owner'], 'old')
    def test_corrupt_or_oversized_gzip_rejected(self):
        self.client.items['bad.gz'] = (b'not gzip', '1')
        with self.assertRaises(gzip.BadGzipFile): self.store.get('bad.gz')
        self.store.put('large.gz', {'text': 'x'*200})
        self.cfg.max_object = 100
        with self.assertRaises(ValueError): self.store.get('large.gz')
    def test_duplicate_and_nested_prefixes_rejected(self):
        for value in ('cost-analysis/raw', 'cost-analysis/raw/sub'):
            with patch.dict(os.environ, {'CURATED_PREFIX': value}):
                with self.assertRaises(ValueError): Config()


class SDKCompatibilityTests(unittest.TestCase):
    def test_real_sdk_accepts_used_parameters(self):
        signer = MagicMock(spec=oci.auth.signers.SecurityTokenSigner)
        kwargs = {'signer': signer, 'retry_strategy': oci.retry.NoneRetryStrategy(), 'timeout': (10, 120)}
        usage = oci.usage_api.UsageapiClient({'region': 'us-phoenix-1'}, **kwargs)
        usage.base_client.call_api = MagicMock(return_value=SimpleNamespace(headers={}))
        from datetime import datetime, timezone
        details = oci.usage_api.models.RequestSummarizedUsagesDetails(tenant_id='t',
            time_usage_started=datetime(2026, 1, 1, tzinfo=timezone.utc),
            time_usage_ended=datetime(2026, 1, 2, tzinfo=timezone.utc),
            granularity='DAILY', query_type='USAGE_ONLY',
            group_by=['compartmentId', 'service', 'skuPartNumber', 'unit'], compartment_depth=6)
        usage.request_summarized_usages(details, page='next', limit=1000)
        self.assertTrue(usage.base_client.call_api.called)
        storage = oci.object_storage.ObjectStorageClient({'region': 'us-phoenix-1'}, **kwargs)
        storage.base_client.call_api = MagicMock(return_value=SimpleNamespace(headers={}))
        storage.put_object('n', 'b', 'o', b'{}', if_none_match='*', content_type='application/json')
        storage.delete_object('n', 'b', 'o', if_match='etag')
        agent = oci.generative_ai_agent.GenerativeAiAgentClient({'region': 'us-phoenix-1'}, **kwargs)
        agent.base_client.call_api = MagicMock(return_value=SimpleNamespace(headers={}))
        agent.list_data_ingestion_jobs(compartment_id='c', data_source_id='d', display_name='name', limit=100)
        agent.create_data_ingestion_job(oci.generative_ai_agent.models.CreateDataIngestionJobDetails(
            compartment_id='c', data_source_id='d', display_name='name'), opc_retry_token='token')
        agent.get_data_ingestion_job('job')
