import scrapy
from jimmy_crawler.items import JimmyCrawlerItem


class BaseJimmySpider(scrapy.Spider):
    """
    Parent class for all law portals.
    """

    def __init__(self, *args, **kwargs):
        super(BaseJimmySpider, self).__init__(*args, **kwargs)
        self.run_id = kwargs.get('run_id', 'manual_run')

    def build_item(self, response, title, **kwargs):
        """
        Helper to create a standardized LawItem.
        """
        item = JimmyCrawlerItem()

        # 1. Standard fields
        item['url'] = response.url
        item['portal_id'] = self.name
        item['run_id'] = self.run_id
        item['title'] = title.strip() if title else None

        # 2. Fill in optional fields from kwargs
        for key, value in kwargs.items():
            if key in item.fields:
                item[key] = value
            else:
                self.logger.warning(f"Field '{key}' is not in JimmyCrawlerItem. Skipping.")

        return item