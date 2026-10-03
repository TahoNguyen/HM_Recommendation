"""Bounded-memory CSV import. Publish source manifest only after all imports succeed."""
import argparse
import csv
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from pymongo import MongoClient, InsertOne
from dotenv import load_dotenv

def import_dataset(args):
    if args.limit < 0:
        raise ValueError('limit must be >= 0')
    root = Path(args.data_path)
    files = {'articles': 'articles.csv', 'customers': 'customers.csv', 'transactions': 'transactions_train.csv'}
    for filename in files.values():
        if not (root / filename).is_file():
            raise FileNotFoundError(root / filename)
    with MongoClient(os.getenv('MONGODB_URI', 'mongodb://localhost:27017')) as client:
        db = client[os.getenv('MONGODB_DATABASE', 'hm_recommendation')]
        run = uuid4().hex
        collections = {key: f'raw_{key}_{run}' for key in files}
        counts = {}
        for key, filename in files.items():
            coll = db[collections[key]]
            count, batch = 0, []
            with (root / filename).open(encoding='utf-8-sig', newline='') as stream:
                for row in csv.DictReader(stream):
                    if key == 'transactions' and args.limit and count >= args.limit:
                        break
                    # Keep exact duplicate rows: they can represent multiple units.
                    # CSV is read as strings so leading zeros are never discarded.
                    row['_id'] = count
                    batch.append(InsertOne(row))
                    count += 1
                    if len(batch) >= 5000:
                        coll.bulk_write(batch, ordered=False)
                        batch.clear()
                        if count % 500000 == 0:
                            print(f'{key}: {count:,}', flush=True)
                if batch:
                    coll.bulk_write(batch, ordered=False)
            counts[key] = count
            coll.create_index('article_id' if key == 'articles' else 'customer_id')
        db[collections['transactions']].create_index([('t_dat', 1), ('sales_channel_id', 1)])
        db.metadata.replace_one({'_id':'active_source'}, {'_id':'active_source', 'run_id':run, 'collections':collections, 'counts':counts, 'sample':bool(args.limit), 'imported_at':datetime.now(timezone.utc).isoformat()}, upsert=True)
        print({'source_run':run, 'counts':counts, 'sample':bool(args.limit)})

def main():
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-path', default=os.getenv('DATA_PATH', 'data/raw'))
    parser.add_argument('--limit', type=int, default=0, help='0 = full data; positive = first N transaction rows for a smoke test only')
    import_dataset(parser.parse_args())

if __name__ == '__main__':
    main()
