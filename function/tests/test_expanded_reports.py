import os
import tempfile
import unittest
from datetime import date
from unittest.mock import patch
from analytics import Ledger, periods, query, report_sections
from func import Config
from insights import service_review
from test_finops import row, chunk


class ExpandedReportsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ledger = Ledger(self.tmp.name+'/db')
    def tearDown(self):
        self.ledger.close(); self.tmp.cleanup()
    def test_bucket_defaults_and_config_override(self):
        with patch.dict(os.environ, {'TENANCY_OCID': 'test', 'BUCKET_NAME': 'bucket', 'NAMESPACE': 'ns', 'OCI_REGION': 'test-region'}, clear=True):
            cfg = Config()
            self.assertEqual(cfg.bucket, 'bucket')
            self.assertEqual(cfg.namespace, 'ns')
            with patch.dict(os.environ, {'BUCKET_NAME': 'override'}):
                self.assertEqual(Config().bucket, 'override')
    def test_closed_periods_and_leap_year(self):
        plan = {p['id']: p for p in periods(date(2024,1,1), date(2026,10,1))}
        self.assertEqual(plan['previous_calendar_week']['start'], '2026-09-21')
        self.assertEqual(plan['previous_calendar_quarter']['start'], '2026-07-01')
        self.assertEqual(plan['previous_calendar_quarter']['previous_start'], '2026-04-01')
        self.assertEqual(plan['previous_calendar_year']['previous_start'], '2024-01-01')
        self.assertIn('calendar_2026_Q3', plan)
        self.assertIn('last_90_days', plan)
        self.assertEqual(len(plan), len(periods(date(2024,1,1), date(2026,10,1))))
    def test_yoy_and_conditional_advice(self):
        self.ledger.add(chunk([row(day='2025-09-01',value='10')]), {'2025-09-01'})
        self.ledger.add(chunk([row(day='2026-09-01',value='20')]), {'2026-09-01'})
        self.ledger.ready()
        sections = dict(report_sections(self.ledger, {'id':'test','start':'2026-09-01','end':'2026-09-02','previous_start':'2026-08-31'}, 'now'))
        self.assertIn('100.00%', sections['USD_year_over_year'])
        self.assertIn('20.00', sections['USD_optimization_reviews'])
        self.assertIn('utilization', sections['USD_optimization_reviews'])
        self.assertIn('not proven savings', sections['USD_optimization_reviews'])
    def test_missing_days_suppress_optimization_priorities(self):
        self.ledger.add(chunk([row()]), {'2026-09-01','2026-09-02'})
        self.ledger.ready()
        sections = dict(report_sections(self.ledger, {'id':'test','start':'2026-09-01','end':'2026-09-03','previous_start':'2026-08-30'}, 'now'))
        self.assertIn('USD_insight_limits', sections)
        self.assertNotIn('USD_optimization_reviews', sections)
    def test_weekend_and_weekday_averages(self):
        self.ledger.add(chunk([row(day='2026-09-04',value='10'), row(day='2026-09-05',value='30')]), {'2026-09-04','2026-09-05'})
        self.ledger.ready()
        sections = dict(report_sections(self.ledger, {'id':'test','start':'2026-09-04','end':'2026-09-06','previous_start':'2026-09-02'}, 'now'))
        pattern=sections['USD_weekday_weekend']
        self.assertIn('| 1 | 10.00 | 10.00 |', pattern)
        self.assertIn('| 1 | 30.00 | 30.00 |', pattern)
    def test_bottom_n_and_full_total(self):
        a=row(value='10'); b=row(value='30'); b['service']='Storage'
        self.ledger.add(chunk([a,b]), {'2026-09-01'}); self.ledger.ready()
        q=query(self.ledger, {'start_date':'2026-09-01','end_date':'2026-09-02','order':'ascending','limit':1})
        part=q['partitions'][0]
        self.assertEqual(part['rows'][0]['service'],'Compute')
        self.assertEqual(part['total_value_all_groups'],'40')
        self.assertEqual(part['total_groups'],2)
    def test_service_specific_validation(self):
        self.assertIn('Database', service_review('Autonomous Database')[0])
        self.assertIn('retention', service_review('Object Storage')[1])
        self.assertIn('latency', service_review('Network')[1])
