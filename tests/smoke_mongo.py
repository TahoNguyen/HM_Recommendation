"""Exercise CSV -> real MongoDB -> Spark -> MongoDB -> API Store in an isolated DB.

Run from Docker with tests mounted at /app/tests. Never touches the project DB.
"""
import csv
import os
import tempfile
from argparse import Namespace
from pathlib import Path
from uuid import uuid4
from pymongo import MongoClient
from training.import_data import import_dataset
from training.pipeline import run
from backend.store import Store

def write_csv(root, filename, fields, rows):
    with (root / filename).open('w', encoding='utf-8', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

def main():
    database = 'hm_pipeline_smoke_' + uuid4().hex
    os.environ['MONGODB_DATABASE'] = database
    os.environ['APP_MODE'] = 'mongo'
    os.environ['SPARK_MASTER'] = 'local[2]'
    os.environ['SPARK_PARTITIONS'] = '4'
    with MongoClient(os.environ['MONGODB_URI']) as client:
        try:
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                fields = ['article_id','product_code','prod_name','product_type_name','product_group_name','colour_group_name','detail_desc']
                write_csv(root,'articles.csv',fields,[dict(zip(fields,[f'010877501{i}',f'108775{i}',f'Smoke product {i}','Shirt','Garment Upper body','Blue','Synthetic validation data'])) for i in range(3)])
                write_csv(root,'customers.csv',['customer_id'],[{'customer_id':'smoke-customer'}])
                rows=[]
                for day in range(1,8):
                    for article_id in ['0108775010', '0108775011' if day <= 4 or day == 7 else '0108775012']:
                        rows.append({'t_dat':f'2020-01-{day:02d}','customer_id':'smoke-customer','article_id':article_id,'price':'0.02','sales_channel_id':'1'})
                write_csv(root,'transactions_train.csv',list(rows[0]),rows)
                import_dataset(Namespace(data_path=str(root),limit=0))
                result = run(Namespace(min_support=.1,min_confidence=.1,min_lift=0,cutoff='2020-01-06'))
                assert result['transactions']==14, result
                assert result['baskets']==6, result
                assert result['evaluation']['baskets']==1, result
                assert result['rules']>0, result
                store=Store()
                try:
                    assert store.manifest()['run_id']==result['run_id']
                    assert store.products()['total']==3
                    assert store.product('0108775010')['article_id']=='0108775010'
                    recommendations=store.recommend(['0108775010'])
                    assert recommendations['source']=='fp_growth'
                    assert recommendations['items'], recommendations
                finally:
                    store.client.close()
                print('PASS: isolated real MongoDB / Spark / FP-Growth / API Store integration',flush=True)
                candidate = run(Namespace(min_support=.1,min_confidence=.1,min_lift=0,cutoff='2020-01-04',
                    validation_end='2020-01-06',seed=42,evaluation_limit=100,no_activate=True,item_level='product_code',
                    sweep_configs='0.0001:0.3,0.0001:0.15,0.00005:0.15'))
                assert candidate['execution']['reused_clean_data'], candidate
                assert len(candidate['validation']['results'])==3, candidate
                assert candidate['evaluation']['baskets']==1, candidate
                assert client[database].metadata.find_one({'_id':'active_model'})['run_id']==result['run_id']
                from training.audit_model import audit
                candidate_store=Store()
                try:
                    audited=audit(candidate_store,candidate)
                    assert audited['status']=='passed' and audited['item_level']=='product_code'
                    assert candidate_store.manifest()['run_id']==result['run_id']
                finally:
                    candidate_store.client.close()
                client[database].metadata.replace_one({'_id':'active_model'},candidate)
                grouped_store=Store()
                try:
                    grouped = grouped_store.recommend(['0108775010'])
                    assert grouped['item_level']=='product_code', grouped
                    assert grouped['items'], grouped
                    assert all(len(r['product']['article_id'])==10 and r['product']['product_code']!='1087750' for r in grouped['items'])
                    # No rule can fire from all selected groups: fallback must exclude them too.
                    fallback=grouped_store.recommend(['0108775010','0108775011','0108775012'])
                    assert not fallback['items'] and not fallback['suggestions'], fallback
                finally:
                    grouped_store.client.close()
                print('PASS: validation sweep / reusable data / candidate activation / grouped SKU rendering',flush=True)
        finally:
            # Only this uniquely named, synthetic DB created by the test is removed.
            client.drop_database(database)

if __name__ == '__main__':
    main()
