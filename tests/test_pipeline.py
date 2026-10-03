"""Meaningful Spark checks using a tiny dataset, independent of MongoDB."""
import importlib.util
import os
import unittest
os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')
READY = all(importlib.util.find_spec(name) for name in ('pyspark','pymongo','dotenv'))

@unittest.skipUnless(READY, 'Install requirements and Java to test Spark')
class PipelineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import sys
        os.environ['PYSPARK_PYTHON'] = sys.executable
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master('local[2]').appName('HM_Pipeline_Check')
            .config('spark.driver.host','127.0.0.1').config('spark.driver.bindAddress','127.0.0.1')
            .config('spark.sql.shuffle.partitions','2').config('spark.ui.enabled','false').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_ids_cleaning_duplicates_and_channel_baskets(self):
        from training.pipeline import clean_transactions, make_baskets
        rows=[('2020-09-01','c1','108775015','0.01','1'),
              ('2020-09-01','c1','108775015','0.01','1'),
              ('2020-09-01','c1','108775016','0.02','1'),
              ('2020-09-01','c1','108775017','0.03','2'),
              ('bad-date','c1','108775015','0.01','1'),
              ('2020-09-01','c1','108775015','NaN','1'),
              ('2020-09-01','c1','108775015','-1','1'),
              ('2020-09-01','c1','12345678901','0.01','1'),
              ('2020-09-01','c1','999999999','0.01','1')]
        values=','.join('('+','.join("'"+s+"'" for s in row)+')' for row in rows)
        raw=self.spark.sql('SELECT * FROM VALUES '+values+' AS t(t_dat,customer_id,article_id,price,sales_channel_id)')
        articles=self.spark.sql("SELECT * FROM VALUES ('0108775015'),('0108775016'),('0108775017') AS t(article_id)")
        clean=clean_transactions(raw,articles)
        self.assertEqual(clean.count(),4)
        baskets=make_baskets(clean).collect()
        self.assertEqual(len(baskets),2)
        self.assertEqual(sorted(r.basket_size for r in baskets),[1,2])
        self.assertEqual(next(r.items for r in baskets if r.sales_channel_id==1),['0108775015','0108775016'])

    def test_fpgrowth_support_and_temporal_evaluation(self):
        from pyspark.ml.fpm import FPGrowth
        from training.pipeline import evaluate
        # A occurs in all 4 baskets, B occurs in 2. A->B confidence must be .5.
        train=self.spark.sql("SELECT * FROM VALUES (array('a','b')),(array('a','b')),(array('a')),(array('a')) AS t(items)")
        model=FPGrowth(itemsCol='items',minSupport=.2,minConfidence=.1).fit(train)
        rules=model.associationRules
        r=next(r for r in rules.collect() if r.antecedent==['a'] and r.consequent==['b'])
        self.assertAlmostEqual(r.support,.5)
        self.assertAlmostEqual(r.confidence,.5)
        future=self.spark.sql("SELECT 'customer' customer_id, DATE '2020-09-20' t_dat, 1 sales_channel_id, array('a','b') items, 2 basket_size")
        result=evaluate(future,rules)
        self.assertEqual(result['baskets'],1)
        self.assertEqual(result['recall_at_6'],1)
        self.assertAlmostEqual(result['precision_at_6'],1/6)
        self.assertEqual(result['coverage'],1)

    def test_seeded_masking_and_temporal_boundaries(self):
        from training.pipeline import prepare_cases, temporal_split, selection_key
        from pyspark.sql import functions as F
        baskets=(self.spark.range(30).select(F.col('id').cast('string').alias('customer_id'),
            F.expr("date_add(DATE '2020-09-01', cast(id % 3 as int))").alias('t_dat'),
            F.lit(1).alias('sales_channel_id'), F.array(F.lit('a'),F.lit('b'),F.lit('c')).alias('items'),F.lit(3).alias('basket_size')))
        first=prepare_cases(baskets,30,42).orderBy('key').collect()
        reordered=prepare_cases(baskets.withColumn('items',F.reverse('items')),30,42).orderBy('key').collect()
        self.assertEqual([(r.key,r.target) for r in first],[(r.key,r.target) for r in reordered])
        self.assertGreater(len({r.target for r in first}),1)
        self.assertTrue(all(r.target not in r.context and len(r.context)==2 for r in first))
        train,val,test=temporal_split(baskets,'2020-09-01','2020-09-02')
        self.assertEqual([train.count(),val.count(),test.count()],[10,10,10])
        with self.assertRaises(ValueError):temporal_split(baskets,'2020-09-02','2020-09-01')
        row={'min_support':.001,'min_confidence':.3,'validation':{'recall_at_6':.1,'precision_at_6':.01,'coverage':.2}}
        self.assertEqual(selection_key(row),selection_key({**row,'test':{'recall_at_6':1}}))

    def test_family_baskets_collapse_variants_and_preserve_channels(self):
        from training.pipeline import group_baskets
        sku=self.spark.sql("SELECT * FROM VALUES ('c', DATE '2020-09-01',1,array('0108775001','0108775002','0208775001')),('c',DATE '2020-09-01',2,array('0108775001')) AS t(customer_id,t_dat,sales_channel_id,items)")
        articles=self.spark.sql("SELECT * FROM VALUES ('0108775001','0108775'),('0108775002','0108775'),('0208775001','0208775') AS t(article_id,product_code)")
        rows=group_baskets(sku,articles).collect()
        self.assertEqual(len(rows),2)
        self.assertEqual(next(r.items for r in rows if r.sales_channel_id==1),['0108775','0208775'])
        self.assertEqual(next(r.basket_size for r in rows if r.sales_channel_id==2),1)

    def test_custom_sweep_keeps_support_and_confidence_valid(self):
        from training.pipeline import parse_sweep_configs
        self.assertEqual(parse_sweep_configs('0.00002:0.1,0.00002:0.15,0.00002:0.1'),[(.00002,.1),(.00002,.15)])
        for value in ('0:0.1','0.01:1.1','nan:0.1','0.1:nan','0.1'):
            with self.assertRaises(ValueError):parse_sweep_configs(value)

    def test_popularity_and_hybrid_evaluation(self):
        from training.pipeline import evaluate, prepare_cases
        from pyspark import StorageLevel
        baskets=self.spark.sql("SELECT 'customer' customer_id, DATE '2020-09-20' t_dat, 1 sales_channel_id, array('a','b') items, 2 basket_size")
        rules=self.spark.sql("SELECT array('a') antecedent, array('b') consequent, .5D confidence, 2D lift, .1D support WHERE false")
        popularity=self.spark.sql("SELECT * FROM VALUES ('a',10L),('b',9L),('c',8L) AS t(candidate,count)")
        result=evaluate(baskets,rules,popularity=popularity)
        self.assertEqual(result['coverage'],0)
        self.assertEqual(result['popularity']['recall_at_6'],1)
        self.assertEqual(result['hybrid']['recall_at_6'],1)
        self.assertEqual(result['hybrid']['coverage'],1)
        cases=prepare_cases(baskets).persist(StorageLevel.DISK_ONLY)
        try:
            repeated=evaluate(baskets,rules,popularity=popularity,cases=cases)
            self.assertEqual(result,repeated)
            self.assertTrue(cases.is_cached, 'An evaluator must not evict the shared validation cases')
        finally:
            cases.unpersist()

if __name__=='__main__':
    unittest.main()
