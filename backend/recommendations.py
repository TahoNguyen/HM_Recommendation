"""Rule matching shared by the MongoDB API, demo, and tests."""
def rank_recommendations(rules, items, limit=6):
    basket = set(items)
    best = {}
    for rule in rules:
        antecedent = set(rule.get('antecedent', []))
        if not antecedent or not antecedent.issubset(basket):
            continue
        for article_id in set(rule.get('consequent', [])) - basket:
            candidate = {key: rule[key] for key in ('confidence', 'lift', 'support')}
            candidate.update(article_id=article_id, antecedent=sorted(antecedent))
            rank = (candidate['confidence'], candidate['lift'], candidate['support'])
            old = best.get(article_id)
            if old is None or rank > (old['confidence'], old['lift'], old['support']):
                best[article_id] = candidate
    return sorted(best.values(), key=lambda r: (-r['confidence'], -r['lift'], -r['support'], r['article_id']))[:limit]
