"""Cost accounting for finite, serial replay; missing measurements stay missing."""
import math
from benefits import ENGINES


def complete_sum(values):
    return sum(values) if values and all(v is not None and math.isfinite(v) and v >= 0 for v in values) else None


def break_even(upfront, native_query, asap_query, incremental_maintenance):
    if any(v is None for v in (upfront, native_query, asap_query, incremental_maintenance)):
        return {'refreshes': None, 'reason': 'incomplete CPU evidence'}
    saved = native_query - asap_query - incremental_maintenance
    if saved <= 0:
        return {'refreshes': None, 'reason': 'no positive net CPU saving per refresh'}
    return {'refreshes': max(0, math.ceil(upfront / saved)), 'net_cpu_saved_per_refresh_ns': saved,
            'assumption': 'same query mix and maintenance CPU per refresh as this finite replay; excludes future drift'}


def summarize_costs(phases, records, lifetime=None):
    result = {}
    refresh_count = len({r['repeat'] for r in records})
    for engine in ENGINES:
        rows = [r['engines'][engine] for r in records]
        eligible = bool(rows) and all(r['passed'] for r in rows)
        arms = {}
        for arm in ('native', 'asap'):
            selected = [p for p in phases if p['engine'] == engine and
                        (p['arm'] == arm or (arm == 'asap' and p['arm'] == 'fallback'))]
            def phase_cpu(p):
                return p.get('compiler_cpu_ns') if p['phase'] == 'planning' else p.get('cpu_ns')
            upfront = [p for p in selected if p['phase'] != 'maintenance']
            maintenance = [p for p in selected if p['phase'] == 'maintenance']
            arms[arm] = {
                'upfront_cpu_ns': complete_sum([phase_cpu(p) if p['complete'] else None for p in upfront]),
                'maintenance_cpu_ns': complete_sum([phase_cpu(p) if p['complete'] else None for p in maintenance]) if maintenance else 0,
                'maintenance_batches': len(maintenance),
                'query_cpu_ns': complete_sum([r[arm].get('cpu_ns') for r in rows]),
                'upfront_wall_ns': complete_sum([p['wall_ns'] if p['complete'] else None for p in upfront]),
                'maintenance_wall_ns': complete_sum([p['wall_ns'] if p['complete'] else None for p in maintenance]) if maintenance else 0,
                'query_wall_ns': complete_sum([r[arm]['elapsed_ns'] for r in rows]),
            }
            arms[arm]['accounted_phase_cpu_ns'] = complete_sum([arms[arm][k] for k in ('upfront_cpu_ns', 'maintenance_cpu_ns', 'query_cpu_ns')])
            arms[arm]['accounted_serial_wall_ns'] = complete_sum([arms[arm][k] for k in ('upfront_wall_ns', 'maintenance_wall_ns', 'query_wall_ns')])
        native, asap = arms['native'], arms['asap']
        subtract = lambda a, b: a - b if a is not None and b is not None else None
        per_refresh = lambda value: value / refresh_count if value is not None and refresh_count else None
        amortization = break_even(subtract(asap['upfront_cpu_ns'], native['upfront_cpu_ns']),
                                 per_refresh(native['query_cpu_ns']), per_refresh(asap['query_cpu_ns']),
                                 per_refresh(subtract(asap['maintenance_cpu_ns'], native['maintenance_cpu_ns'])))
        if not eligible:
            amortization = {'refreshes': None, 'reason': 'failed or unequal query results'}
        total = (lifetime or {}).get(engine)
        def ratio(a, b):
            return a / b if eligible and a is not None and b is not None and b > 0 else None
        result[engine] = {'observed_refreshes': refresh_count,
                          'native_over_asap_phase_cpu_ratio': ratio(native['accounted_phase_cpu_ns'], asap['accounted_phase_cpu_ns']),
                          'native_over_asap_serial_total_time_ratio': ratio(native['accounted_serial_wall_ns'], asap['accounted_serial_wall_ns']),
                          'native_over_asap_lifetime_cpu_ratio': ratio(total.get('native_cpu_ns'), total.get('asap_plus_fallback_and_planner_cpu_ns')) if total else None,
                          'arms': arms, 'cpu_break_even': amortization, 'lifetime': total,
                          'comparison_eligible': eligible,
                          'scope': 'phase CPU includes whole backend and fallback processes; phase sums exclude gaps; lifetime includes gaps and background work; serial wall time is not CPU or concurrent throughput'}
    return result
