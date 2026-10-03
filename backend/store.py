import json
import os
import re
from pathlib import Path
from backend.recommendations import rank_recommendations

ROOT = Path(__file__).resolve().parents[1]

class Store:
    def __init__(self):
        self.mode = os.getenv('APP_MODE', 'demo')
        if self.mode not in ('demo', 'mongo'):
            raise ValueError('APP_MODE must be demo or mongo')
        if self.mode == 'demo':
            self.fixture = json.loads((ROOT / 'data/demo.json').read_text(encoding='utf-8'))
        else:
            from pymongo import MongoClient
            self.client = MongoClient(os.getenv('MONGODB_URI', 'mongodb://localhost:27017'), serverSelectionTimeoutMS=4000)
            self.db = self.client[os.getenv('MONGODB_DATABASE', 'hm_recommendation')]

    def manifest(self):
        if self.mode == 'demo':
            return self.fixture['metrics']
        self.client.admin.command('ping')
        result = self.db.metadata.find_one({'_id': 'active_model'}, {'_id': 0})
        if not result:
            raise RuntimeError('Chưa có mô hình. Hãy nhập dữ liệu và chạy training.pipeline.')
        return result

    def collection(self, name, manifest):
        return self.db[manifest['collections'][name]]

    def eligible_products(self, meta):
        """Products that can trigger a rule when viewed on their own."""
        if self.mode == 'demo':
            return {r['antecedent'][0] for r in self.fixture['rules'] if len(r.get('antecedent',[]))==1}
        cache = getattr(self, '_eligible_cache', None)
        if cache is None or cache[0] != meta['run_id']:
            eligible = set(self.collection('rules',meta).distinct('antecedent', {'antecedent':{'$size':1}}))
            cache = (meta['run_id'],eligible)
            self._eligible_cache = cache
        return cache[1]

    def metrics(self):
        meta = self.manifest()
        eligible = self.eligible_products(meta)
        field = 'product_code' if meta.get('item_level')=='product_code' else 'article_id'
        if self.mode == 'demo':
            covered = sum(p[field] in eligible for p in self.fixture['products'])
        else:
            covered = self.collection('products',meta).count_documents({field:{'$in':sorted(eligible)}})
        return {**meta, 'mode':self.mode,'recommendable_products':covered,
                'catalog_rule_coverage':covered/meta['products'] if meta['products'] else 0}

    def products(self, query='', category='', sort='popular', page=1, page_size=12, buy_together=False):
        meta = self.manifest()
        eligible = self.eligible_products(meta)
        field = 'product_code' if meta.get('item_level')=='product_code' else 'article_id'
        if self.mode == 'demo':
            rows = [p for p in self.fixture['products'] if (not buy_together or p[field] in eligible) and (not category or p['category'] == category) and query.casefold() in (p['name'] + ' ' + p['article_id'] + ' ' + p['category']).casefold()]
            if sort == 'name':
                rows.sort(key=lambda p: p['name'])
            else:
                rows.sort(key=lambda p: p.get('purchase_count', 0), reverse=True)
            total = len(rows)
            rows = rows[(page-1)*page_size:page*page_size]
        else:
            condition = {}
            if buy_together:
                condition[field] = {'$in':sorted(eligible)}
            if query:
                pattern = re.escape(query)
                condition['$or'] = [{f: {'$regex': pattern, '$options': 'i'}} for f in ('name', 'article_id', 'category')]
            if category:
                condition['category'] = category
            coll = self.collection('products', meta)
            total = coll.count_documents(condition)
            order = [('name', 1), ('article_id', 1)] if sort == 'name' else [('purchase_count', -1), ('article_id', 1)]
            rows = list(coll.find(condition, {'_id': 0}).sort(order).skip((page-1)*page_size).limit(page_size))
        rows = [{**p, 'has_buy_together':p.get(field) in eligible} for p in rows]
        return {'items': rows, 'total': total, 'page': page, 'page_size': page_size, 'mode': self.mode}

    def product(self, article_id, meta=None):
        if self.mode == 'demo':
            return next((p for p in self.fixture['products'] if p['article_id'] == article_id), None)
        return self.collection('products', meta or self.manifest()).find_one({'article_id': article_id}, {'_id': 0})

    def categories(self):
        if self.mode == 'demo':
            return sorted(set(p['category'] for p in self.fixture['products']))
        return sorted(self.collection('products', self.manifest()).distinct('category'))

    def recommend(self, items, limit=6):
        meta = self.manifest()
        grouped = meta.get('item_level') == 'product_code'
        selected = [self.product(item,meta) for item in items] if grouped else []
        model_items = sorted({p['product_code'] for p in selected if p}) if grouped else items
        if self.mode == 'demo':
            rules = self.fixture['rules']
        else:
            # The subset check is deliberately done in rank_recommendations.
            rules = self.collection('rules', meta).find({'antecedent': {'$in': model_items}}, {'_id': 0})
        ranked = rank_recommendations(rules, model_items, limit)
        result = []
        for rule in ranked:
            if grouped:
                product = self.collection('products',meta).find_one({'product_code':rule['article_id']},
                    {'_id':0},sort=[('purchase_count',-1),('article_id',1)])
                if product:
                    rule = {**rule, 'product_code':rule['article_id'], 'article_id':product['article_id']}
            else:
                product = self.product(rule['article_id'], meta)
            if product:
                result.append({'product': product, 'rule': rule})
        suggestions = []
        if not result:
            if self.mode == 'demo':
                candidates = sorted(self.fixture['products'],key=lambda p:(-p.get('purchase_count',0),p['article_id']))
            else:
                field = 'product_code' if grouped else 'article_id'
                # Older manifests have unit counts only; new models use basket frequency.
                popularity_field = 'recommendation_popularity' if meta.get('item_level') else 'purchase_count'
                candidates = self.collection('products',meta).find({field:{'$nin':model_items},popularity_field:{'$gt':0}}, {'_id':0})
                if grouped:
                    candidates = self.collection('products',meta).aggregate([
                        {'$match':{field:{'$nin':model_items},popularity_field:{'$gt':0}}},
                        {'$sort':{'purchase_count':-1,'article_id':1}},
                        {'$group':{'_id':'$product_code','product':{'$first':'$$ROOT'}}},
                        {'$replaceRoot':{'newRoot':'$product'}},
                        {'$sort':{popularity_field:-1,'product_code':1}}, {'$limit':limit}, {'$project':{'_id':0}}])
                else:
                    candidates = candidates.sort([(popularity_field,-1),('article_id',1)]).limit(limit)
            for product in candidates:
                if product['article_id'] not in items:
                    suggestions.append({'product':product,'reason':'popular_in_training','source':'popularity'})
                if len(suggestions) == limit:
                    break
        return {'items': result, 'suggestions':suggestions,
                'source': 'demo' if self.mode == 'demo' else 'fp_growth',
                'item_level':meta.get('item_level','article_id'), 'fallback':bool(suggestions)}
