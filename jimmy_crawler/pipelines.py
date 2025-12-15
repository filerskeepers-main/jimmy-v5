import pymongo
import logging
import datetime
from itemadapter import ItemAdapter


class JimmyCrawlerPipeline:
    collection_name = 'jimmy_crawler_data'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.client = None
        self.db = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )

    def open_spider(self, spider):
        # Connecting without SSL params as requested
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        logging.info(f"Connected to MongoDB: {self.mongo_db}")

    def close_spider(self, spider):
        if self.client:
            self.client.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        data = adapter.asdict()

        # REMOVE ONLY Scrapy Internal Fields
        # We keep everything else (not_ro, status, etc.)
        data.pop('file_urls', None)
        data.pop('files', None)

        if data.get('status') is None:
            data['status'] = 'new'  # Default to CrawlingStatus.NEW

        if data.get('version') is None:
            data['version'] = 1

        if data.get('not_ro') is None:
            data['not_ro'] = False  # Default Boolean

        if data.get('total_occurrences') is None:
            data['total_occurrences'] = {}

        if data.get('appeared_keywords') is None:
            data['appeared_keywords'] = []

        # DATE CONVERSION
        # MongoEngine expects datetime objects, not date objects
        date_fields = [
            'date_of_enactment', 'date_of_last_amendment', 'date_of_publication',
            'date_of_effective', 'date_of_expiration', 'date_of_repealed', 'date_of_decision'
        ]
        for field in date_fields:
            val = data.get(field)
            if val and isinstance(val, datetime.date) and not isinstance(val, datetime.datetime):
                # Convert datetime.date -> datetime.datetime (midnight)
                data[field] = datetime.datetime.combine(val, datetime.time.min)

        # UPDATE TIMESTAMP
        data['last_updated_at'] = datetime.datetime.utcnow()

        # UPSERT TO MONGODB
        # Using 'url' as the unique key
        unique_key = {'url': data['url']}

        try:
            self.db[self.collection_name].update_one(
                unique_key,
                {'$set': data},
                upsert=True
            )
            logging.debug(f"Saved: {data['url']}")
        except Exception as e:
            logging.error(f"Error saving to MongoDB: {e}")

        return item