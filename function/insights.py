"""Calculated Finance/CXO reports. Cost-based suggestions are conditional."""
from collections import defaultdict
from datetime import date
from analytics import ZERO, change, days, fmt, number, shift_months, table


def service_review(service):
    name = service.lower()
    if any(s in name for s in ('compute', 'container', 'kubernetes')):
        return ('Cloud/platform owner', 'Check CPU/memory utilization, shape mix, instance counts and runtime. Consider rightsizing or nonproduction schedules only after workload and billing validation.')
    if any(s in name for s in ('database', 'exadata', 'autonomous', 'mysql')):
        return ('Database owner', 'Check workload demand, capacity, autoscaling, backups and license model. Validate performance and recovery requirements before capacity changes.')
    if any(s in name for s in ('storage', 'backup', 'archive')):
        return ('Storage/data owner', 'Check growth, tier mix, replication and retention. Review lifecycle policies against recovery, retrieval-cost and regulatory requirements. Use inventory to identify unused assets.')
    if any(s in name for s in ('network', 'transfer', 'bandwidth', 'load balanc')):
        return ('Network/application owner', 'Inspect traffic growth, regional transfers, replication and architecture. Validate latency, residency and migration costs before changes.')
    return ('Service owner and Finance', 'Check deployment changes, quantities, SKU mix and commercial terms. Establish workload criticality and utilization before proposing reductions.')


def financial_insights(ledger, period, currency, base, coverage):
    a, b, p = period['start'], period['end'], period['previous_start']
    if coverage['days_with_no_cost_records']:
        yield currency+'_insight_limits', base + '\n## Interpretation limits\nUnverified observations prevent reliable calendar patterns and optimization prioritization for the entire interval.\n'
        return
    flt = {'currency': currency}
    services = ledger.aggregate('cost', a, b, ['service'], flt)
    positive = [r for r in services if number(r['value']) > 0]
    positive_sum = sum((number(r['value']) for r in positive), ZERO)
    negative_sum = sum((number(r['value']) for r in services if number(r['value']) < 0), ZERO)
    total = sum((number(r['value']) for r in services), ZERO)
    top_three = sum((number(r['value']) for r in positive[:3]), ZERO)
    text = base + '\n## Spending concentration and negative amounts\n\n'
    text += f'Positive service subtotals: {fmt(positive_sum)}. Negative service subtotals: {fmt(negative_sum)}. Net cost: {fmt(total)}.\n'
    if positive_sum:
        text += f'Top three positive-cost services account for {top_three/positive_sum*100:.2f}% of positive service subtotals. This describes concentration, not waste or resilience.\n'
    text += 'Signs are separated after service aggregation; these are not gross charge/credit line-item totals. Negative amounts are not automatically savings.\n'
    yield currency+'_concentration', text

    current_days = (date.fromisoformat(b)-date.fromisoformat(a)).days
    prior_coverage = ledger.coverage(p, a)
    if prior_coverage['queried_all_days'] and not prior_coverage['days_with_no_cost_records']:
        old = {r['service']: number(r['value']) for r in ledger.aggregate('cost', p, a, ['service'], flt)}
        current = {r['service']: number(r['value']) for r in services}
        old_total = sum(old.values(), ZERO)
        prior_days = (date.fromisoformat(a)-date.fromisoformat(p)).days
        detail = base + '\n## Spending rate comparison\n\n' + table(
            ['Measure', 'Current interval', 'Previous interval'],
            [['Calendar days', current_days, prior_days], ['Net cost', fmt(total), fmt(old_total)],
             ['Cost per day', fmt(total/current_days), fmt(old_total/prior_days)]])
        detail += f'\nChange in daily cost rate: {change(total/current_days, old_total/prior_days)}.\n'
        yield currency+'_daily_rate_comparison', detail
        newly = sorted(k for k, v in current.items() if v != 0 and old.get(k, ZERO) == 0)
        ceased = sorted(k for k, v in old.items() if v != 0 and current.get(k, ZERO) == 0)
        yield currency+'_appearing_disappearing_services', base + '\n## Newly appearing or disappearing nonzero service spend\nPeriod-level observations; not evidence that resources were created or terminated.\n\n' + table(
            ['Service', 'Observation', 'Current', 'Prior'],
            [[k, 'Nonzero now; zero/no prior amount', fmt(current[k]), fmt(old.get(k, ZERO))] for k in newly] +
            [[k, 'Zero/no current amount; nonzero prior', fmt(current.get(k, ZERO)), fmt(old[k])] for k in ceased])

    ya = shift_months(date.fromisoformat(a), -12).isoformat()
    yb = shift_months(date.fromisoformat(b), -12).isoformat()
    yc = ledger.coverage(ya, yb)
    annual = base + f'\n## Year-over-year comparison\nPrior-year interval: {ya} inclusive to {yb} exclusive.\n'
    if ya < yb and yc['queried_all_days'] and not yc['days_with_no_cost_records']:
        old = ledger.aggregate('total', ya, yb, ['currency'], flt)
        old_value = number(old[0]['value']) if old else ZERO
        year_days = (date.fromisoformat(yb)-date.fromisoformat(ya)).days
        annual += table(['Current cost', 'Prior-year cost', 'Absolute change', 'Change %', 'Daily-rate change %'],
                        [[fmt(total), fmt(old_value), fmt(total-old_value), change(total, old_value),
                          change(total/current_days, old_value/year_days)]])
    else:
        annual += 'Unavailable: prior-year coverage is insufficient or unverified. No partial comparison is presented.'
    yield currency+'_year_over_year', annual

    daily = {r['date']: number(r['value']) for r in ledger.aggregate('total', a, b, ['date'], flt)}
    patterns = defaultdict(lambda: [ZERO, 0])
    for day in days(date.fromisoformat(a), date.fromisoformat(b)):
        label = 'Weekend (Saturday/Sunday UTC)' if date.fromisoformat(day).weekday() >= 5 else 'Weekday (Monday-Friday UTC)'
        patterns[label][0] += daily.get(day, ZERO)
        patterns[label][1] += 1
    yield currency+'_weekday_weekend', base + '\n## Weekday and weekend spending\nUTC calendar classification; not evidence of utilization or business working hours.\n\n' + table(
        ['Calendar category', 'Days', 'Cost', 'Cost per day'],
        [[k, count, fmt(cost), fmt(cost/count)] for k, (cost, count) in sorted(patterns.items())])
    reviews = []
    for r in positive[:10]:
        owner, checks = service_review(r['service'])
        reviews.append([r['service'], fmt(r['value']), owner, checks])
    yield currency+'_optimization_reviews', base + '\n## Prioritized optimization investigations\nPrioritized by positive service cost, not proven savings potential. Conditional checks, not findings of waste.\n\n' + table(
        ['Service', 'Observed cost', 'Suggested owner', 'Validation/action'], reviews) + (
        '\n\nFinance: confirm business drivers, budgets and commitments before accepting a savings target. '
        'Engineering: establish utilization, inventory, performance and availability requirements. '
        'No realized or attainable savings amount is asserted from cost data alone.\n')
