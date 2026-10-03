import json
import os
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.request import urlopen
from urllib.error import HTTPError
from backend.recommendations import rank_recommendations
from backend.store import Store
from backend.app import Handler

class RecommendationsTest(unittest.TestCase):
    def test_requires_entire_antecedent_and_excludes_basket(self):
        rules=[{'antecedent':['a','b'],'consequent':['c'],'confidence':.9,'lift':2,'support':.1}]
        self.assertEqual(rank_recommendations(rules,['a']),[])
        self.assertEqual(rank_recommendations(rules,['a','b'])[0]['article_id'],'c')
        self.assertEqual(rank_recommendations(rules,['a','b','c']),[])

    def test_deduplicates_and_ranks_best_rule(self):
        rules=[{'antecedent':['a'],'consequent':['b'],'confidence':c,'lift':2,'support':.1} for c in [.3,.8]]
        result=rank_recommendations(rules,['a'])
        self.assertEqual(len(result),1)
        self.assertEqual(result[0]['confidence'],.8)

    def test_popularity_is_separate_and_excludes_selected_products(self):
        os.environ['APP_MODE']='demo'
        store=Store()
        store.fixture['rules']=[]
        selected=store.fixture['products'][0]['article_id']
        result=store.recommend([selected])
        self.assertEqual(result['items'],[])
        self.assertTrue(result['fallback'])
        self.assertEqual(len(result['suggestions']),6)
        self.assertNotIn(selected,[r['product']['article_id'] for r in result['suggestions']])
        self.assertTrue(all('rule' not in r for r in result['suggestions']))

class APITest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ['APP_MODE']='demo'
        Handler.store=Store()
        cls.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        cls.thread=threading.Thread(target=cls.server.serve_forever,daemon=True)
        cls.thread.start()
        cls.base=f'http://127.0.0.1:{cls.server.server_port}'

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def get(self,path):
        with urlopen(self.base+path) as response:
            return json.load(response)

    def test_catalog_detail_recommendations(self):
        catalog=self.get('/api/products?page_size=4')
        self.assertEqual(len(catalog['items']),4)
        self.assertEqual(catalog['mode'],'demo')
        source=catalog['items'][0]['article_id']
        self.assertEqual(self.get('/api/products/'+source)['article_id'],source)
        rec=self.get('/api/recommendations?items='+source)
        self.assertEqual(rec['source'],'demo')
        self.assertTrue(rec['items'])
        self.assertNotIn(source,[r['product']['article_id'] for r in rec['items']])

    def test_filter_and_validation(self):
        self.assertEqual(self.get('/api/products?q=1000000000')['total'],1)
        self.assertEqual(self.get('/api/products?q=no_such_product')['total'],0)
        for path,status in [('/api/recommendations?items=bad',400),('/api/products?page=bad',400),('/api/products/9999999999',404),('/../.env.example',404),('/api/missing',404)]:
            with self.assertRaises(HTTPError) as ctx:
                self.get(path)
            self.assertEqual(ctx.exception.code,status)

    def test_filter_discovers_products_that_have_real_single_item_rules(self):
        catalog=self.get('/api/products?buy_together=1&page_size=48')
        self.assertGreater(catalog['total'],0)
        for p in catalog['items']:
            self.assertTrue(p['has_buy_together'])
            rec=self.get('/api/recommendations?items='+p['article_id'])
            self.assertTrue(rec['items'])
            self.assertFalse(rec['fallback'])
        metrics=self.get('/api/metrics')
        self.assertEqual(metrics['recommendable_products'],catalog['total'])
        self.assertGreater(metrics['catalog_rule_coverage'],0)
        with self.assertRaises(HTTPError):self.get('/api/products?buy_together=bad')

if __name__=='__main__':
    unittest.main()
