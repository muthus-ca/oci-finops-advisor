"""Deterministic reporting. No LLM arithmetic, live credentials, or SDK dependency."""
import calendar
import json
import sqlite3
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal

ZERO = Decimal('0')
VIEWS = {
    'cost': ('COST', ['compartmentId', 'compartmentPath', 'service', 'currency']),
    'region': ('COST', ['compartmentId', 'service', 'region', 'currency']),
    'total': ('COST', ['currency']),
    'usage': ('USAGE_ONLY', ['compartmentId', 'service', 'skuPartNumber', 'unit']),
}


def shift_months(day, months):
    index = day.year * 12 + day.month - 1 + months
    year, month = divmod(index, 12)
    month += 1
    return date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


def days(start, end):
    while start < end:
        yield start.isoformat()
        start += timedelta(days=1)


def number(value):
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError('Non-finite amount/quantity in API data')
    return result


def normalize(items, view, start, end):
    rows = []
    for item in items:
        if item.get('is_forecast'):
            continue
        day = str(item.get('time_usage_started', ''))[:10]
        if not (start <= day < end):
            raise ValueError('API row outside requested UTC window: ' + day)
        field = 'computed_quantity' if view == 'usage' else 'computed_amount'
        if item.get(field) is None:
            raise ValueError('Missing ' + field + '; refusing to substitute zero/attributed cost')
        denom = 'unit' if view == 'usage' else 'currency'
        if not item.get(denom):
            raise ValueError('Missing ' + denom + ' in API response')
        rows.append({
            'date': day, 'value': str(number(item[field])),
            **{key: str(item.get(key) or '') for key in (
                'currency', 'compartment_id', 'compartment_path', 'service',
                'region', 'sku_part_number', 'unit')}
        })
    return rows


def reconcile(views, absolute=Decimal('0.01'), relative=Decimal('0.000001')):
    sums = {}
    for view in ('total', 'cost', 'region'):
        totals = defaultdict(lambda: ZERO)
        for row in views[view]:
            totals[(row['date'], row['currency'])] += number(row['value'])
        sums[view] = totals
    checks = []
    for key in sorted(set().union(*(set(v) for v in sums.values()))):
        expected = sums['total'][key]
        tolerance = max(absolute, abs(expected) * relative)
        for view in ('cost', 'region'):
            actual = sums[view][key]
            delta = actual - expected
            checks.append({'date': key[0], 'currency': key[1], 'view': view,
                           'reference': str(expected), 'detail': str(actual),
                           'difference': str(delta), 'passed': abs(delta) <= tolerance})
    if any(not check['passed'] for check in checks):
        raise ValueError('Reconciliation failed; raw pages retained. ' + json.dumps(
            [c for c in checks if not c['passed']][:8]))
    return checks


class DecimalSum:
    def __init__(self):
        self.value = ZERO

    def step(self, value):
        self.value += number(value)

    def finalize(self):
        return str(self.value)


class Ledger:
    """Disk-backed scratch ledger; Object Storage manifests are the durable truth."""
    def __init__(self, filename):
        self.db = sqlite3.connect(filename)
        self.db.create_aggregate('decimal_sum', 1, DecimalSum)
        self.db.execute('CREATE TABLE facts (view TEXT, date TEXT, currency TEXT, '
                        'compartment_id TEXT, compartment_path TEXT, service TEXT, '
                        'region TEXT, sku_part_number TEXT, unit TEXT, value TEXT)')
        self.queried = set()
        self.observed = set()

    def add(self, chunk, selected_days):
        self.queried.update(selected_days)
        self.observed.update(r['date'] for r in chunk['views']['total']
                             if r['date'] in selected_days)
        fields = ['date', 'currency', 'compartment_id', 'compartment_path', 'service',
                  'region', 'sku_part_number', 'unit', 'value']
        for view, rows in chunk['views'].items():
            self.db.executemany('INSERT INTO facts VALUES (?,?,?,?,?,?,?,?,?,?)',
                               ((view, *(r[f] for f in fields)) for r in rows
                                if r['date'] in selected_days))

    def ready(self):
        self.db.execute('CREATE INDEX fact_lookup ON facts(view,date)')
        self.db.commit()

    def close(self):
        self.db.close()

    def coverage(self, start, end):
        expected = set(days(date.fromisoformat(start), date.fromisoformat(end)))
        missing = sorted(expected - self.queried)
        no_records = sorted((expected & self.queried) - self.observed)
        return {'queried_all_days': not missing, 'missing_days': missing,
                'days_with_no_cost_records': no_records,
                'note': 'No returned records are not proof of zero spend or historical availability.'}

    def aggregate(self, view, start, end, group, filters=None):
        allowed = {'currency', 'compartment_id', 'compartment_path', 'service',
                   'region', 'sku_part_number', 'unit', 'date'}
        if not group or any(g not in allowed for g in group):
            raise ValueError('Invalid grouping')
        filters = filters or {}
        if any(k not in allowed for k in filters):
            raise ValueError('Invalid filter')
        clauses = ['view=?', 'date>=?', 'date<?']
        args = [view, start, end]
        for key, value in filters.items():
            clauses.append(key + '=?')
            args.append(str(value))
        columns = ','.join(group)
        sql = ('SELECT ' + columns + ',decimal_sum(value) FROM facts WHERE ' +
               ' AND '.join(clauses) + ' GROUP BY ' + columns)
        rows = [dict(zip([*group, 'value'], row)) for row in self.db.execute(sql, args)]
        return sorted(rows, key=lambda r: number(r['value']), reverse=True)


def periods(start, end):
    """end is exclusive; rolling month windows use calendar subtraction."""
    result = []
    for n in (7, 14, 21, 28, 30, 60, 90):
        a = end - timedelta(days=n)
        result.append({'id': 'last_%d_days' % n, 'start': a.isoformat(),
                       'end': end.isoformat(), 'previous_start': (a - timedelta(days=n)).isoformat()})
    for n in (1, 3, 6, 12, 24):
        a = shift_months(end, -n)
        result.append({'id': 'last_%d_months' % n, 'start': a.isoformat(),
                       'end': end.isoformat(), 'previous_start': shift_months(a, -n).isoformat()})
    anchors = {'month_to_date': end.replace(day=1),
               'quarter_to_date': date(end.year, ((end.month-1)//3)*3+1, 1),
               'year_to_date': date(end.year, 1, 1),
               'previous_calendar_year': date(end.year-1, 1, 1)}
    for label, a in anchors.items():
        b = date(end.year, 1, 1) if label == 'previous_calendar_year' else end
        if a < b:
            result.append({'id': label, 'start': a.isoformat(), 'end': b.isoformat(),
                           'previous_start': (shift_months(a, -12) if label == 'previous_calendar_year' else a - (b-a)).isoformat()})
    cursor = start.replace(day=1)
    while cursor < end:
        nxt = shift_months(cursor, 1)
        result.append({'id': 'calendar_' + cursor.strftime('%Y_%m'),
                       'start': cursor.isoformat(), 'end': min(nxt, end).isoformat(),
                       'previous_start': shift_months(cursor, -1).isoformat()})
        cursor = nxt
    # Closed week/month/quarter reports have unambiguous adjacent comparison periods.
    monday = end - timedelta(days=end.weekday())
    month = end.replace(day=1)
    quarter = date(end.year, ((end.month-1)//3)*3+1, 1)
    for name, a, b, prior in (
        ('previous_calendar_week', monday-timedelta(days=7), monday, monday-timedelta(days=14)),
        ('previous_calendar_month', shift_months(month, -1), month, shift_months(month, -2)),
        ('previous_calendar_quarter', shift_months(quarter, -3), quarter, shift_months(quarter, -6))):
        result.append({'id': name, 'start': a.isoformat(), 'end': b.isoformat(), 'previous_start': prior.isoformat()})
    cursor = date(start.year, ((start.month-1)//3)*3+1, 1)
    while shift_months(cursor, 3) <= end:
        result.append({'id': f'calendar_{cursor.year}_Q{(cursor.month-1)//3+1}',
                       'start': cursor.isoformat(), 'end': shift_months(cursor, 3).isoformat(),
                       'previous_start': shift_months(cursor, -3).isoformat()})
        cursor = shift_months(cursor, 3)
    return result


def fmt(value):
    return format(number(value), ',.2f')


def change(current, previous):
    if previous == 0:
        return 'not defined (prior value is zero)'
    if previous < 0:
        return 'not shown (prior value is negative)'
    return format((current - previous) / previous * 100, '.2f') + '%'


def cell(value):
    return str(value).replace('|', '\\|').replace('\n', ' ').replace('\r', ' ')


def table(headers, rows):
    return '\n'.join(['| ' + ' | '.join(map(cell, headers)) + ' |',
                      '| ' + ' | '.join('---' for _ in headers) + ' |'] +
                     ['| ' + ' | '.join(map(cell, row)) + ' |' for row in rows])


def report_sections(ledger, period, generated, include_usage=True):
    """Each section is independently dated so retrieval preserves its meaning."""
    a, b, p = period['start'], period['end'], period['previous_start']
    coverage = ledger.coverage(a, b)
    previous_coverage = ledger.coverage(p, a)
    header = (f"# OCI FinOps: {period['id']}\n\n"
              f"UTC interval: {a} inclusive to {b} exclusive.\n"
              f"Generated UTC: {generated}. Source: OCI Usage API; query type COST.\n"
              "Compartment scope follows the configured Usage API depth; boundary levels can include descendants.\n"
              "Independent breakdowns describe the same spend: never add them together.\n")
    if not coverage['queried_all_days']:
        yield 'coverage', header + '\nRequested interval is unavailable in full. No partial total is presented.\n' + json.dumps(coverage)
        return
    common = header + ('\nCoverage warning: %d queried dates returned no cost records; '
                       'do not assume historical completeness or zero spend.\n' %
                       len(coverage['days_with_no_cost_records']) if coverage['days_with_no_cost_records'] else '')
    yield 'coverage', common + '\n## Data coverage\n\n' + json.dumps(coverage)
    totals = ledger.aggregate('total', a, b, ['currency'])
    prior_ok = (previous_coverage['queried_all_days'] and not previous_coverage['days_with_no_cost_records']
                and not coverage['days_with_no_cost_records'])
    for total in totals:
        currency = total['currency']
        amount = number(total['value'])
        base = common + f'\nCurrency: {currency}. Period cost: {fmt(amount)}.\nPrior comparison interval, when available: {p} inclusive to {a} exclusive.\n'
        flt = {'currency': currency}
        prior = ledger.aggregate('total', p, a, ['currency'], flt) if prior_ok else []
        overview = base + '\n## Executive overview\n\n'
        span = (date.fromisoformat(b)-date.fromisoformat(a)).days
        overview += f'Daily average: {fmt(amount / span)} {currency} across {span} calendar days.\n'
        if prior_ok:
            old = number(prior[0]['value']) if prior else ZERO
            overview += (f'Previous interval: {p} to {a} exclusive; total {fmt(old)}; '
                         f'change {fmt(amount-old)} ({change(amount, old)}). '
                         'Calendar periods can have different lengths; daily averages are preferable for rate comparisons.\n')
        else:
            overview += 'Prior-period comparison unavailable: insufficient or unverified history.\n'
        if not coverage['days_with_no_cost_records'] and amount >= 0:
            overview += (f'Simple 30-day run-rate scenario: {fmt(amount / span * 30)} {currency}. '
                         'Assumes this period\'s daily average persists; not an OCI forecast or savings commitment.\n')
        yield currency + '_overview', overview
        for group, title in [(['compartment_id'], 'Compartments'),
                             (['service'], 'Services')]:
            rows = ledger.aggregate('cost', a, b, [*group, 'currency'], flt)
            labels = defaultdict(set)
            if title == 'Compartments':
                for label_row in ledger.aggregate('cost', a, b, ['compartment_id', 'compartment_path'], flt):
                    labels[label_row['compartment_id']].add(label_row['compartment_path'])
            old_rows = ledger.aggregate('cost', p, a, [*group, 'currency'], flt) if prior_ok else []
            # Identity uses ID for compartments; path changes must not create false new spend.
            key_field = group[0]
            old_map = {r[key_field]: number(r['value']) for r in old_rows}
            current_map = {r[key_field]: number(r['value']) for r in rows}
            for offset in range(0, len(rows), 40):
                data = []
                for i, row in enumerate(rows[offset:offset+40], offset+1):
                    val = number(row['value'])
                    old = old_map.get(row[key_field], ZERO)
                    share = format(val/amount*100, '.2f')+'%' if amount > 0 else 'not defined'
                    display = [row[g] for g in group]
                    if title == 'Compartments':
                        display.append('; '.join(sorted(labels[row['compartment_id']])))
                    data.append([i, *display, fmt(val), share,
                                 fmt(val-old) if prior_ok else 'unavailable',
                                 change(val, old) if prior_ok else 'unavailable'])
                yield currency+'_'+title+'_'+str(offset), base + f'\n## {title} ranked by cost\n\n' + table(
                    ['Rank', *group, *(['Paths during interval'] if title == 'Compartments' else []), 'Cost', 'Share of net total', 'Change vs prior', 'Change %'], data)
            if prior_ok:
                movers = sorted(set(old_map) | set(current_map),
                                key=lambda k: abs(current_map.get(k, ZERO)-old_map.get(k, ZERO)), reverse=True)
                yield currency+'_'+title+'_movers', base + f'\n## Largest {title.lower()} changes\nPrior interval: {p} to {a} exclusive.\n\n' + table(
                    [key_field, 'Current', 'Prior', 'Change'],
                    [[k, fmt(current_map.get(k, ZERO)), fmt(old_map.get(k, ZERO)),
                      fmt(current_map.get(k, ZERO)-old_map.get(k, ZERO))] for k in movers[:20]])
        detail = ledger.aggregate('cost', a, b, ['compartment_id', 'service', 'currency'], flt)
        paths = defaultdict(set)
        for entry in ledger.aggregate('cost', a, b, ['compartment_id', 'compartment_path'], flt):
            paths[entry['compartment_id']].add(entry['compartment_path'])
        grouped = defaultdict(list)
        for row in detail:
            grouped[row['compartment_id']].append(row)
        for comp, rows in sorted(grouped.items()):
            path = '; '.join(sorted(paths[comp]))
            for offset in range(0, len(rows), 40):
                yield currency+'_comp_'+comp+'_'+str(offset), base + f'\n## Services in compartment {path}\nCompartment ID: {comp}\n\n' + table(
                    ['Service', 'Cost'], [[r['service'], fmt(r['value'])] for r in rows[offset:offset+40]])
        regional = ledger.aggregate('region', a, b, ['region', 'service', 'currency'], flt)
        for offset in range(0, len(regional), 40):
            yield currency+'_regional_'+str(offset), base + '\n## Regional service cost\n\n' + table(
                ['Region', 'Service', 'Cost'], [[r['region'], r['service'], fmt(r['value'])] for r in regional[offset:offset+40]])
        daily = {r['date']: number(r['value']) for r in ledger.aggregate('total', a, b, ['date', 'currency'], flt)}
        observed = sorted(daily)
        # Flag candidates relative to the previous seven returned daily observations.
        candidates = []
        for i in range(7, len(observed)):
            day = observed[i]
            prior_days = observed[i-7:i]
            if (date.fromisoformat(day)-date.fromisoformat(prior_days[0])).days != 7:
                continue
            avg = sum((daily[d] for d in prior_days), ZERO)/7
            if avg > 0 and daily[day] > avg * Decimal('1.5'):
                candidates.append([day, fmt(daily[day]), fmt(avg), change(daily[day], avg)])
        yield currency+'_spikes', base + '\n## Daily spike candidates\nRule: more than 50% above the preceding seven calendar days; a heuristic, not proof of waste.\n\n' + (
            table(['Day', 'Cost', 'Prior 7-day mean', 'Increase'], candidates) if candidates else 'No candidates met this rule.')
        for offset in range(0, len(observed), 40):
            yield currency+'_daily_'+str(offset), base + '\n## Daily actual costs\n\n' + table(
                ['UTC day', 'Cost'], [[d, fmt(daily[d])] for d in observed[offset:offset+40]])
        from insights import financial_insights
        yield from financial_insights(ledger, period, currency, base, coverage)
    if include_usage:
        rows = ledger.aggregate('usage', a, b, ['compartment_id', 'service', 'sku_part_number', 'unit'])
        # No ranking across units or SKUs. Partitioning makes this explicit to the agent.
        groups = defaultdict(list)
        for row in rows:
            groups[(row['service'], row['sku_part_number'], row['unit'])].append(row)
        for (service, sku, unit), members in sorted(groups.items()):
            for offset in range(0, len(members), 40):
                yield 'usage_'+service+'_'+sku+'_'+unit+'_'+str(offset), common + (
                    f'\n## Usage: {service}; SKU {sku}; unit {unit}\n'
                    'Source query type: USAGE_ONLY. Compare quantities only within this SKU and unit.\n\n') + table(
                        ['Compartment ID', 'Quantity'], [[r['compartment_id'], r['value']] for r in members[offset:offset+40]])


def query(ledger, request):
    start, end = request['start_date'], request['end_date']
    if date.fromisoformat(start) >= date.fromisoformat(end):
        raise ValueError('start_date must precede exclusive end_date')
    if (date.fromisoformat(end)-date.fromisoformat(start)).days > 3660:
        raise ValueError('Maximum query interval is 3660 days')
    metric = request.get('metric', 'cost')
    filters = request.get('filters', {})
    group = list(dict.fromkeys(request.get('group_by', ['service'])))
    if metric == 'cost':
        view = 'region' if 'region' in group or 'region' in filters else 'cost'
        supported = {'date', 'currency', 'compartment_id', 'service',
                     'region' if view == 'region' else 'compartment_path'}
        mandatory = ['currency']
    elif metric == 'usage':
        view = 'usage'
        supported = {'date', 'compartment_id', 'service', 'sku_part_number', 'unit'}
        mandatory = ['service', 'sku_part_number', 'unit']
    else:
        raise ValueError('metric must be cost or usage')
    if not group or not set(group).issubset(supported) or not set(filters).issubset(supported):
        raise ValueError('Unsupported dimensions for this metric/view: '+str(sorted(supported)))
    for field in mandatory:
        if field not in group:
            group.append(field)
    coverage = ledger.coverage(start, end)
    if not coverage['queried_all_days']:
        return {'status': 'insufficient_history', 'coverage': coverage}
    rows = ledger.aggregate(view, start, end, group, filters)
    limit = int(request.get('limit', 20))
    if not 1 <= limit <= 1000:
        raise ValueError('limit must be 1..1000')
    order = request.get('order', 'descending')
    if order not in ('ascending', 'descending'):
        raise ValueError('order must be ascending or descending')
    if order == 'ascending':
        rows.reverse()
    # Partition result ranking by currency, or service/SKU/unit for usage.
    partitions = defaultdict(list)
    for row in rows:
        partitions[tuple(row[g] for g in mandatory)].append(row)
    return {'status': 'success', 'start_inclusive': start, 'end_exclusive': end,
            'metric': metric, 'filters': filters, 'group_by': group, 'coverage': coverage, 'order': order,
            'partitions': [{'dimensions': dict(zip(mandatory, k)), 'total_groups': len(v),
                            'returned_groups': min(limit, len(v)), 'total_value_all_groups': str(sum((number(r['value']) for r in v), ZERO)), 'rows': v[:limit]}
                           for k, v in sorted(partitions.items())],
            'note': 'Ranks are within each partition. Compartment scope follows the configured Usage API depth; no additional local descendant rollup.'}
