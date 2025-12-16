import pymongo
import logging
import datetime
from itemadapter import ItemAdapter


class JimmyCrawlerPipeline:
    collection_name = 'jimmy_crawler_data'
    jobs_collection = 'crawl_jobs'  # Where we store run stats

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.client = None
        self.db = None
        self.run_id = None
        self.portal_id = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

        # 1. Capture IDs from the Spider instance
        # These are passed by Scrapyd/FastAPI or default to 'manual'
        self.run_id = getattr(spider, 'run_id', 'manual_run')
        self.portal_id = spider.name

        logging.info(f"Connected to MongoDB. Run ID: {self.run_id}")

        # 2. Initialize/Update the Job Record in MongoDB
        # This allows your Dashboard to see "Running" status immediately
        self.db[self.jobs_collection].update_one(
            {'run_id': self.run_id},
            {
                '$set': {
                    'portal_id': self.portal_id,
                    'status': 'running',
                    'start_time': datetime.datetime.utcnow(),
                    'last_updated': datetime.datetime.utcnow()
                },
                '$setOnInsert': {
                    'items_count': 0,
                    'errors_count': 0
                }
            },
            upsert=True
        )

    def close_spider(self, spider):
        # 3. Mark Job as Finished
        if self.db:
            self.db[self.jobs_collection].update_one(
                {'run_id': self.run_id},
                {
                    '$set': {
                        'status': 'finished',
                        'end_time': datetime.datetime.utcnow()
                    }
                }
            )
            self.client.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        data = adapter.asdict()

        # --- EXISTING CLEANUP LOGIC ---
        data.pop('file_urls', None)
        data.pop('files', None)

        defaults = {
            'status': 'new',
            'version': 1,
            'not_ro': False,
            'total_occurrences': {},
            'appeared_keywords': []
        }
        for key, val in defaults.items():
            if data.get(key) is None:
                data[key] = val

        # Ensure run_id is attached to the item data itself
        if not data.get('run_id'):
            data['run_id'] = self.run_id

        # Date Conversion
        date_fields = [
            'date_of_enactment', 'date_of_last_amendment', 'date_of_publication',
            'date_of_effective', 'date_of_expiration', 'date_of_repealed', 'date_of_decision'
        ]
        for field in date_fields:
            val = data.get(field)
            if val and isinstance(val, datetime.date) and not isinstance(val, datetime.datetime):
                data[field] = datetime.datetime.combine(val, datetime.time.min)

        data['last_updated_at'] = datetime.datetime.utcnow()

        # --- UPSERT ITEM DATA ---
        unique_key = {'url': data['url']}
        try:
            self.db[self.collection_name].update_one(
                unique_key,
                {'$set': data},
                upsert=True
            )

            # --- NEW: INCREMENT JOB STATS ---
            # This makes the "Items Scraped" counter live on your dashboard
            self.db[self.jobs_collection].update_one(
                {'run_id': self.run_id},
                {
                    '$inc': {'items_count': 1},
                    '$set': {'last_updated': datetime.datetime.utcnow()}
                }
            )

            logging.debug(f"Saved: {data['url']}")
        except Exception as e:
            logging.error(f"Error saving to MongoDB: {e}")
            # Optional: Increment error count in job stats
            self.db[self.jobs_collection].update_one(
                {'run_id': self.run_id},
                {'$inc': {'errors_count': 1}}
            )

        return item
