"""Read-only audit of the active model; output contains no customer identifiers."""
import json
import argparse
import math
import os
import re
from datetime import date
from pathlib import Path
from dotenv import load_dotenv
from backend.store import Store


def audit(store, meta=None):
    if store.mode != 'mongo':
        raise ValueError('Audit requires APP_MODE=mongo')
    meta = meta or store.manifest()
    counts = {key:store.collection(key,meta).count_documents({}) for key in ('products','rules','itemsets')}
    assert all(counts[key] == meta[key] for key in counts), 'Collection counts differ from manifest'
    assert any(i.get('unique') for i in store.collection('products',meta).list_indexes()), 'Product unique index missing'
    field = meta.get('item_level','article_id')
    digits = 7 if field=='product_code' else 10
    ids = set(store.collection('products',meta).distinct(field))
    assert all(isinstance(i,str) and re.fullmatch(r'\d{%d}' % digits,i) for i in ids), 'Invalid product identifiers'
    params = meta['parameters']
    for rule in store.collection('rules',meta).find({}, {'_id':0}):
        a,b = set(rule['antecedent']),set(rule['consequent'])
        assert a and b and not a.intersection(b) and a.union(b).issubset(ids), 'Invalid rule references'
        assert params['min_confidence']-1e-12 <= rule['confidence'] <= 1, 'Confidence outside bounds'
        assert params['min_support']-1e-12 <= rule['support'] <= 1, 'Support outside bounds'
        assert math.isfinite(rule['lift']) and rule['lift'] > params['min_lift'], 'Invalid lift'
    validation = meta.get('validation')
    if validation:
        assert date.fromisoformat(meta['cutoff']) < date.fromisoformat(validation['end'])
        from training.pipeline import selection_key
        best = max(validation['results'],key=selection_key)
        assert best['min_support']==params['min_support'] and best['min_confidence']==params['min_confidence']
    evaluation = meta['evaluation']
    assert evaluation['protocol']=='temporal_seeded_masked_item' and evaluation['baskets']>0
    for score in (evaluation,evaluation['popularity'],evaluation['hybrid']):
        assert 0 <= score['coverage'] <= 1
        assert math.isclose(score['recall_at_6'],score['hits']/evaluation['baskets'])
        assert math.isclose(score['precision_at_6'],score['hits']/(6*evaluation['baskets']))
    transactions = list(store.collection('clean_transactions',meta).aggregate([{'$sample':{'size':1000}},{'$project':{'_id':0}}]))
    for row in transactions:
        assert isinstance(row['t_dat'],date) and row['customer_id'].strip()
        assert re.fullmatch(r'\d{10}',row['article_id'])
        assert math.isfinite(row['price']) and row['price']>0 and row['sales_channel_id'] in (1,2)
    baskets = list(store.collection('baskets',meta).aggregate([{'$sample':{'size':1000}},{'$project':{'_id':0,'items':1,'basket_size':1}}]))
    assert all(len(r['items'])==len(set(r['items']))==r['basket_size'] and r['basket_size']>=1 for r in baskets)
    images = Path(os.getenv('DATA_PATH','data/raw')).resolve()/'images'
    top = list(store.collection('products',meta).find({}, {'_id':0,'image_url':1}).sort([('purchase_count',-1),('article_id',1)]).limit(100))
    missing_images = sum(not (images/p['image_url'].removeprefix('/images/')).is_file() for p in top)
    eligible=store.eligible_products(meta)
    covered=store.collection('products',meta).count_documents({field:{'$in':sorted(eligible)}})
    return {'status':'passed','run_id':meta['run_id'],'item_level':field,'counts':counts,
            'clean_transactions_sample_checked':len(transactions),'baskets_sample_checked':len(baskets),
            'rules_checked':counts['rules'],'recommendable_products':covered,'catalog_rule_coverage':covered/meta['products'],
            'image_sample':{'top_products_checked':len(top),'missing':missing_images},
            'evaluation':evaluation,'validation':validation,'parameters':params}


def main():
    load_dotenv()
    parser=argparse.ArgumentParser()
    parser.add_argument('--run-id',help='Audit a saved candidate without activating it')
    args=parser.parse_args()
    store = Store()
    try:
        meta=None
        if args.run_id:
            meta=store.db.model_runs.find_one({'_id':args.run_id})
            if not meta:
                raise ValueError('Model run not found')
        print(json.dumps(audit(store,meta),ensure_ascii=True,default=str))
    finally:
        if hasattr(store,'client'):
            store.client.close()


if __name__=='__main__':
    main()
