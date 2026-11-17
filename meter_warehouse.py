"""Existing ordinary cumulative-meter query; revisions and durable state remain."""
from fractions import Fraction


class MeterWarehouse:
    def __init__(self, load, modulus, rate):
        self.load, self.modulus, self.rate = load, modulus, rate
        self.points = []

    def put(self, points):
        self.points.extend(points)

    def query(self, product, start, end, max_gap):
        points = sorted(self.points, key=lambda point: point['at'])
        energy, known = {}, Fraction(0)
        cuts = sorted({start, end, *[x['start'] for x in product['slices']
                                   if start < x['start'] < end]})
        consumed = []
        for left, right in zip(points, points[1:]):
            duration = right['at'] - left['at']
            delta = right['value'] - left['value']
            if (duration <= 0 or duration > max_gap or right.get('reset')
                    or delta < 0 or delta > self.rate * duration):
                continue
            lo, hi = max(start, left['at']), min(end, right['at'])
            if lo >= hi:
                continue
            borders = [lo] + [x for x in cuts if lo < x < hi] + [hi]
            for a, b in zip(borders, borders[1:]):
                state = max((x for x in product['slices'] if x['start'] <= a),
                            key=lambda x: x['start'])
                owner = state['owners'][self.load]
                consumed.append({'start': a, 'end': b, 'owner': owner})
                key = owner if owner is not None else 'DISCONNECTED'
                energy[key] = energy.get(key, Fraction(0)) + Fraction(delta * (b-a), duration)
                known += b-a
        return {'product_id': product['product_id'],
                'energy': {k: str(v) for k, v in sorted(energy.items())},
                'known_minutes': str(known), 'unknown_minutes': str(end-start-known),
                'consumed': consumed}
