"""MongoDB -> Spark -> baskets -> FP-Growth -> versioned MongoDB outputs."""
import argparse
import os
import sys
import logging
from datetime import date, datetime, timezone
from uuid import uuid4
from dotenv import load_dotenv
from pymongo import MongoClient
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.ml.fpm import FPGrowth
from pyspark import StorageLevel

CONNECTOR = 'org.mongodb.spark:mongo-spark-connector_2.12:10.4.1'

def clean_transactions(df, article_ids):
    return (df.filter(F.trim('article_id').rlike('^[0-9]{1,10}$')).select(F.to_date('t_dat').alias('t_dat'), F.trim('customer_id').alias('customer_id'),
                      F.lpad(F.trim('article_id'), 10, '0').alias('article_id'),
                      F.col('price').cast('double').alias('price'), F.col('sales_channel_id').cast('int').alias('sales_channel_id'))
        .filter(F.col('t_dat').isNotNull() & (F.length('customer_id') > 0) & F.col('article_id').rlike('^[0-9]{10}$')
                & F.col('price').isNotNull() & ~F.isnan('price') & (F.col('price') > 0)
                & (F.col('price') < float('inf')) & F.col('sales_channel_id').isin(1, 2))
        .join(F.broadcast(article_ids), 'article_id', 'left_semi'))

def make_baskets(transactions):
    return (transactions.groupBy('customer_id', 't_dat', 'sales_channel_id')
        .agg(F.sort_array(F.collect_set('article_id')).alias('items'))
        .withColumn('basket_size', F.size('items')))

def group_baskets(sku_baskets, articles):
    expanded = sku_baskets.withColumn('article_id', F.explode('items')).join(
        F.broadcast(articles.select('article_id','product_code')), 'article_id')
    return (expanded.groupBy('customer_id','t_dat','sales_channel_id')
        .agg(F.sort_array(F.collect_set('product_code')).alias('items'))
        .withColumn('basket_size',F.size('items')))

def parse_sweep_configs(value=None):
    if not value:
        return [(0.0001,0.30),(0.0001,0.15),(0.00005,0.15)]
    choices=[]
    for pair in value.split(','):
        support,confidence = map(float,pair.split(':'))
        if not 0 < support <= 1 or not 0 <= confidence <= 1:
            raise ValueError('Sweep support must be in (0,1], confidence in [0,1]')
        if (support,confidence) not in choices:
            choices.append((support,confidence))
    return choices

def prepare_cases(baskets, limit=10000, seed=42):
    # Stable hash order masks different items instead of always the greatest ID.
    test = (baskets.filter(F.col('basket_size') >= 2)
        .withColumn('key', F.sha2(F.concat_ws('|', 'customer_id', F.col('t_dat').cast('string'), F.col('sales_channel_id').cast('string')), 256))
        .withColumn('sample_order', F.xxhash64('key', F.lit(seed)))
        .orderBy('sample_order', 'key').limit(limit)
        .withColumn('masked_order', F.sort_array(F.transform('items', lambda item:
            F.struct(F.xxhash64('key', item, F.lit(seed)).alias('hash'), item.alias('item')))))
        .withColumn('target', F.element_at('masked_order', 1)['item'])
        .withColumn('context', F.array_except('items', F.array('target'))))
    return test.select('key', 'target', 'context')

def evaluate(baskets, rules, limit=10000, seed=42, popularity=None, cases=None):
    owns_cases = cases is None
    test = cases if cases is not None else prepare_cases(baskets, limit, seed).persist(StorageLevel.DISK_ONLY)
    total = test.count()
    def scores(predictions):
        result = predictions.agg(F.countDistinct('key').alias('covered'),
            F.sum(F.when(F.col('candidate') == F.col('target'), 1).otherwise(0)).alias('hits')).first()
        hits = result['hits'] or 0
        return {'precision_at_6':hits/(6*total), 'recall_at_6':hits/total,
                'coverage':result['covered']/total, 'hits':hits}
    base = {'baskets':total, 'protocol':'temporal_seeded_masked_item', 'seed':seed,
            'sample_limit':limit, 'k':6}
    if total == 0:
        if owns_cases:
            test.unpersist()
        return {**base,'precision_at_6':0.0,'recall_at_6':0.0,'coverage':0.0,'status':'no_eligible_baskets'}
    keyed_rules = rules.withColumn('join_id', F.element_at('antecedent', 1))
    matches = (test.withColumn('join_id', F.explode('context')).join(keyed_rules, 'join_id')
        .filter(F.size(F.array_except('antecedent', 'context')) == 0)
        .withColumn('candidate', F.explode('consequent'))
        .filter(~F.array_contains('context', F.col('candidate'))))
    best = (matches.withColumn('rn', F.row_number().over(Window.partitionBy('key','candidate').orderBy(F.desc('confidence'),F.desc('lift'),F.desc('support'))))
        .filter('rn = 1').drop('rn')
        .withColumn('rank', F.row_number().over(Window.partitionBy('key').orderBy(F.desc('confidence'),F.desc('lift'),F.desc('support'),'candidate')))
        .filter('rank <= 6'))
    best = best.select('key','target','candidate').persist(StorageLevel.DISK_ONLY)
    try:
        result = {**base, **scores(best)}
        if popularity is not None:
            # Six plus the largest context guarantees enough eligible candidates.
            max_context = test.agg(F.max(F.size('context'))).first()[0] or 0
            pool = popularity.orderBy(F.desc('count'),'candidate').limit(6+max_context)
            popular = (test.crossJoin(F.broadcast(pool))
                .filter(~F.array_contains('context', F.col('candidate')))
                .withColumn('rank', F.row_number().over(Window.partitionBy('key').orderBy(F.desc('count'),'candidate')))
                .filter('rank <= 6').select('key','target','candidate'))
            result['popularity'] = scores(popular)
            # Runtime fallback applies only when no rule can be rendered.
            fallback = popular.join(best.select('key').distinct(), 'key', 'left_anti')
            result['hybrid'] = scores(best.unionByName(fallback))
        return result
    finally:
        best.unpersist()
        if owns_cases:
            test.unpersist()

def temporal_split(baskets, train_end, validation_end):
    if date.fromisoformat(train_end) >= date.fromisoformat(validation_end):
        raise ValueError('train_end must precede validation_end')
    return (baskets.filter(F.col('t_dat') <= F.lit(train_end)),
            baskets.filter((F.col('t_dat') > F.lit(train_end)) & (F.col('t_dat') <= F.lit(validation_end))),
            baskets.filter(F.col('t_dat') > F.lit(validation_end)))

def selection_key(row):
    # Test metrics and rule counts never participate in model selection.
    score = row['validation']
    return (score['recall_at_6'], score['precision_at_6'], score['coverage'],
            row['min_support'], row['min_confidence'])

def run(args):
    uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
    database = os.getenv('MONGODB_DATABASE', 'hm_recommendation')
    client = MongoClient(uri)
    db = client[database]
    source = db.metadata.find_one({'_id':'active_source'})
    previous = db.metadata.find_one({'_id':'active_model'})
    item_level = getattr(args, 'item_level', 'article_id')
    seed = getattr(args, 'seed', 42)
    evaluation_limit = getattr(args, 'evaluation_limit', 10000)
    validation_end = getattr(args, 'validation_end', None)
    reuse = (not getattr(args, 'no_reuse', False) and previous
             and previous.get('source_run') == (source or {}).get('run_id')
             and previous.get('basket_definition') == 'customer_id + t_dat + sales_channel_id; include singletons')
    if not source:
        raise RuntimeError('Run python -m training.import_data first.')
    master = os.getenv('SPARK_MASTER', 'local[4]')
    partitions = int(os.getenv('SPARK_PARTITIONS', '64'))
    if partitions < 1:
        raise ValueError('SPARK_PARTITIONS must be positive')
    os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
    builder = (SparkSession.builder.appName('HM_MongoDB_FP_Growth')
        .master(master)
        .config('spark.jars.packages', CONNECTOR)
        .config('spark.driver.memory', os.getenv('SPARK_DRIVER_MEMORY', '4g'))
        .config('spark.sql.shuffle.partitions', str(partitions))
        .config('spark.sql.adaptive.coalescePartitions.enabled', 'false')
        .config('spark.local.dir', os.getenv('SPARK_LOCAL_DIR', '/tmp/hm-spark') if os.name != 'nt' else os.getenv('SPARK_LOCAL_DIR', os.path.join(os.getenv('TEMP', '.'), 'hm-spark')))
        .config('spark.mongodb.read.connection.uri', uri)
        .config('spark.mongodb.write.connection.uri', uri))
    if master.startswith('local'):
        # Windows hostnames may contain underscores and are not valid Spark URLs.
        builder = builder.config('spark.driver.host','127.0.0.1').config('spark.driver.bindAddress','127.0.0.1')
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    run_id = uuid4().hex
    outputs = {key:f'{key}_{run_id}' for key in ('products','rules','itemsets','baskets','clean_transactions')}
    def read(key):
        # Explicit strings prevent Mongo inference from losing zero-prefixed IDs.
        schemas = {
            'articles':'article_id string, product_code string, prod_name string, product_type_name string, product_group_name string, colour_group_name string, detail_desc string',
            'transactions':'t_dat string, customer_id string, article_id string, price string, sales_channel_id string'}
        return (spark.read.format('mongodb').option('database', database).option('collection',source['collections'][key]).schema(schemas[key]).load())
    def write(df, key):
        (df.write.format('mongodb').mode('overwrite').option('database',database).option('collection',outputs[key]).save())
    persisted = []
    def disk_cache(df):
        persisted.append(df.persist(StorageLevel.DISK_ONLY))
        return persisted[-1]
    def progress(message):
        print(f'[{datetime.now(timezone.utc).isoformat()}] {message}', flush=True)
    try:
        progress(f'run={run_id}; source={source["run_id"]}; master={master}; partitions={partitions}; heap={os.getenv("SPARK_DRIVER_MEMORY", "4g")}')
        articles = (read('articles').filter(F.trim('article_id').rlike('^[0-9]{1,10}$')).withColumn('article_id', F.lpad(F.trim('article_id'),10,'0'))
            .withColumn('product_code', F.when(F.trim('product_code').rlike('^[0-9]{1,7}$'), F.lpad(F.trim('product_code'),7,'0'))
                        .otherwise(F.substring('article_id',1,7)))
            .filter(F.col('article_id').rlike('^[0-9]{10}$')).dropDuplicates(['article_id']).persist(StorageLevel.MEMORY_AND_DISK))
        persisted.append(articles)
        progress('Cleaning all MongoDB transactions; persisting large frames on disk')
        if reuse:
            progress('Reusing complete cleaned transactions and SKU baskets from the same source run')
            for key in ('clean_transactions', 'baskets'):
                outputs[key] = previous['collections'][key]
            def read_saved(key, schema):
                return spark.read.format('mongodb').option('database',database).option('collection',outputs[key]).schema(schema).load()
            transactions = disk_cache(read_saved('clean_transactions','t_dat date, customer_id string, article_id string, price double, sales_channel_id int'))
            sku_baskets = disk_cache(read_saved('baskets','t_dat date, customer_id string, sales_channel_id int, items array<string>, basket_size int'))
            transaction_count = previous['transactions']
        else:
            transactions = disk_cache(clean_transactions(read('transactions'), articles.select('article_id')))
            transaction_count = transactions.count()
            sku_baskets = disk_cache(make_baskets(transactions))
        progress(f'Clean transactions: {transaction_count:,}; building baskets')
        baskets = sku_baskets
        if item_level == 'product_code':
            progress('Grouping SKU variants into product families; deduplicating each family within a basket')
            baskets = disk_cache(group_baskets(sku_baskets,articles))
        train_transactions = transactions.filter(F.col('t_dat') <= F.lit(args.cutoff)) if args.cutoff else transactions
        train_baskets = baskets.filter(F.col('t_dat') <= F.lit(args.cutoff)) if args.cutoff else baskets
        # Include singleton baskets: support/confidence describe all shopping sessions.
        # The original notebook excluded these, inflating conditional rates.
        # Reuse persisted baskets rather than caching a duplicate items-only copy.
        train = train_baskets.select('items')
        basket_count = train.count()
        progress(f'Training baskets: {basket_count:,}; fitting FP-Growth')
        if basket_count == 0:
            raise ValueError('No training baskets. Check data and --cutoff.')
        popularity = disk_cache(train.select(F.explode('items').alias('candidate')).groupBy('candidate').count())
        validation_results = []
        if validation_end:
            _, validation_baskets, test_baskets = temporal_split(baskets, args.cutoff, validation_end)
            validation_cases = disk_cache(prepare_cases(validation_baskets, evaluation_limit, seed))
            if validation_cases.count() == 0:
                raise ValueError('Validation has no multi-item baskets; choose different dates.')
            choices = parse_sweep_configs(getattr(args,'sweep_configs',None))
            models = {}
            rules_by_support = {}
            for support in sorted({s for s,c in choices}, reverse=True):
                confidence = min(c for s,c in choices if s == support)
                progress(f'Validation sweep: support={support}, confidence>={confidence}')
                candidate_model = FPGrowth(itemsCol='items', minSupport=support, minConfidence=confidence, numPartitions=partitions).fit(train)
                models[support] = candidate_model
                # freqItemsets is lazy: persist it before generating rules, or
                # rule counts/export can mine the complete dataset repeatedly.
                disk_cache(candidate_model.freqItemsets)
                all_rules = disk_cache(candidate_model.associationRules.filter(F.col('lift') > args.min_lift))
                rules_by_support[support] = all_rules
                for s,c in choices:
                    if s != support:
                        continue
                    candidate_rules = all_rules.filter(F.col('confidence') >= c)
                    validation_results.append({'min_support':s, 'min_confidence':c,
                        'rules':candidate_rules.count(), 'validation':evaluate(validation_baskets,candidate_rules,
                        evaluation_limit,seed,popularity,validation_cases)})
                    row = validation_results[-1]
                    progress(f'Validation complete: support={s}, confidence={c}, rules={row["rules"]}, recall@6={row["validation"]["recall_at_6"]:.6f}, coverage={row["validation"]["coverage"]:.6f}')
            selected = max(validation_results,key=selection_key)
            args.min_support, args.min_confidence = selected['min_support'], selected['min_confidence']
            model = models[args.min_support]
            base_rules = rules_by_support[args.min_support]
            progress(f'Selected using validation only: support={args.min_support}, confidence={args.min_confidence}')
        else:
            model = FPGrowth(itemsCol='items', minSupport=args.min_support, minConfidence=args.min_confidence, numPartitions=partitions).fit(train)
            disk_cache(model.freqItemsets)
            base_rules = model.associationRules
            test_baskets = baskets.filter(F.col('t_dat') > F.lit(args.cutoff)) if args.cutoff else None
        rules = disk_cache(base_rules.filter((F.col('lift') > args.min_lift) & (F.col('confidence') >= args.min_confidence)))
        progress('FP-Growth fitted; evaluating and saving versioned results')
        evaluation = evaluate(test_baskets, rules, evaluation_limit, seed, popularity) if args.cutoff else None
        stats = train_transactions.groupBy('article_id').agg(F.count('*').alias('purchase_count'), F.avg('price').alias('price_normalized'))
        popularity_stats = popularity.withColumnRenamed('candidate',item_level).withColumnRenamed('count','recommendation_popularity')
        products = (articles.join(stats,'article_id','left').join(popularity_stats,item_level,'left').select('article_id','product_code',
            F.coalesce('prod_name', 'article_id').alias('name'),
            F.coalesce('product_group_name', F.lit('Other')).alias('category'),
            F.col('product_type_name').alias('product_type'), F.col('colour_group_name').alias('colour'),
            F.col('detail_desc').alias('description'), F.coalesce('purchase_count',F.lit(0)).alias('purchase_count'), 'price_normalized',
            F.coalesce('recommendation_popularity',F.lit(0)).alias('recommendation_popularity'),
            F.concat(F.lit('/images/'),F.substring('article_id',1,3),F.lit('/'),F.col('article_id'),F.lit('.jpg')).alias('image_url')))
        if not reuse:
            write(transactions, 'clean_transactions')
            write(sku_baskets, 'baskets')
        write(products, 'products')
        write(rules, 'rules')
        write(model.freqItemsets.withColumn('support',F.col('freq')/basket_count), 'itemsets')
        db[outputs['products']].create_index('article_id',unique=True)
        db[outputs['products']].create_index([('category',1),('purchase_count',-1)])
        db[outputs['products']].create_index([('recommendation_popularity',-1),(item_level,1)])
        db[outputs['products']].create_index([('product_code',1),('purchase_count',-1),('article_id',1)])
        db[outputs['rules']].create_index('antecedent')
        top_categories = (train_transactions.join(articles.select('article_id','product_group_name'),'article_id')
            .groupBy('product_group_name').count().orderBy(F.desc('count')).limit(10).collect())
        dates = train_transactions.agg(F.min('t_dat').alias('start'),F.max('t_dat').alias('end')).first()
        manifest = {'_id':'active_model', 'run_id':run_id, 'source_run':source['run_id'], 'sample':source['sample'],
            'trained_at':datetime.now(timezone.utc).isoformat(), 'collections':outputs,
            'transactions':transaction_count, 'training_transactions':train_transactions.count(),
            'baskets':basket_count, 'products':products.count(), 'rules':rules.count(), 'itemsets':model.freqItemsets.count(),
            'date_range':f"{dates['start']} → {dates['end']}",
            'parameters':{'min_support':args.min_support,'min_confidence':args.min_confidence,'min_lift':args.min_lift},
            'evaluation':evaluation, 'item_level':item_level,
            'validation':{'end':validation_end,'results':validation_results,'selected_by':'FP-Growth recall, precision, coverage on validation only'} if validation_end else None,
            'top_categories':[{'category':r['product_group_name'] or 'Other','count':r['count']} for r in top_categories],
            'basket_definition':'customer_id + t_dat + sales_channel_id; include singletons', 'cutoff':args.cutoff,
            'execution':{'master':master,'partitions':partitions,'large_frame_storage':'DISK_ONLY','reused_clean_data':bool(reuse)}}
        # Atomic pointer update: API never mixes products and rules from different runs.
        db.model_runs.insert_one({**manifest,'_id':run_id})
        if not getattr(args, 'no_activate', False):
            db.metadata.replace_one({'_id':'active_model'},manifest,upsert=True)
        print({k:v for k,v in manifest.items() if k not in ('collections','top_categories')})
        return manifest
    finally:
        # A dead JVM must not hide the original stage failure with a stop() error.
        try:
            for df in persisted:
                df.unpersist()
            spark.stop()
        except Exception:
            logging.warning('Spark cleanup failed; JVM may have already stopped. See the first error above.')
        finally:
            client.close()

def main():
    load_dotenv()
    parser=argparse.ArgumentParser()
    parser.add_argument('--min-support',type=float,default=float(os.getenv('MIN_SUPPORT','0.0001')))
    parser.add_argument('--min-confidence',type=float,default=float(os.getenv('MIN_CONFIDENCE','0.3')))
    parser.add_argument('--min-lift',type=float,default=float(os.getenv('MIN_LIFT','1')))
    parser.add_argument('--cutoff',help='YYYY-MM-DD; train on/before date, evaluate on later baskets')
    parser.add_argument('--validation-end',help='Sweep thresholds on (cutoff, validation-end]; evaluate selected model only after validation-end')
    parser.add_argument('--sweep-configs',help='Comma-separated support:confidence pairs; requires --validation-end')
    parser.add_argument('--seed',type=int,default=42)
    parser.add_argument('--evaluation-limit',type=int,default=10000)
    parser.add_argument('--item-level',choices=['article_id','product_code'],default='article_id')
    parser.add_argument('--no-reuse',action='store_true',help='Rebuild clean data instead of reusing the same source run')
    parser.add_argument('--no-activate',action='store_true',help='Save a candidate without replacing the web model')
    args=parser.parse_args()
    if not 0 < args.min_support <= 1 or not 0 <= args.min_confidence <= 1 or args.min_lift < 0:
        parser.error('Invalid support/confidence/lift')
    if args.cutoff:
        date.fromisoformat(args.cutoff)
    if args.evaluation_limit < 1:
        parser.error('evaluation-limit must be positive')
    if args.validation_end:
        if not args.cutoff or date.fromisoformat(args.cutoff) >= date.fromisoformat(args.validation_end):
            parser.error('--validation-end requires an earlier --cutoff')
    if args.sweep_configs:
        if not args.validation_end:
            parser.error('--sweep-configs requires --validation-end')
        try:
            parse_sweep_configs(args.sweep_configs)
        except ValueError as exc:
            parser.error(str(exc))
    run(args)

if __name__ == '__main__':
    main()
