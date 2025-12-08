import pymongo
from itemadapter import ItemAdapter
import datetime


class JimmyCrawlerPipeline:
    '''
    This pipeline is added as boilerplate, will add functionality later.
    '''
    collection_name = 'raw_items'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        # 1. Convert Scrapy Item to standard Python Dict
        adapter = ItemAdapter(item)
        data = adapter.asdict()

        # 2. Add technical metadata (Not business logic, just crawler metadata)
        data['crawled_at'] = datetime.datetime.now(datetime.UTC)
        data['crawler_name'] = spider.name

        # 3. Insert into MongoDB
        # We use 'update_one' with upsert=True so we don't create duplicates 
        # if we crawl the same URL twice.
        self.db[self.collection_name].update_one(
            {"url": data["url"]},  # Unique Key
            {"$set": data},  # Update fields
            upsert=True  # Create if doesn't exist
        )

        return item