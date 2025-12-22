import json
import scrapy
from jimmy_crawler.items import JimmyCrawlerItem


class BaseJimmySpider(scrapy.Spider):
    """
    Parent class for all law portals.
    """

    def __init__(self, config_json=None, *args, **kwargs):
        super(BaseJimmySpider, self).__init__(*args, **kwargs)

        self.run_id = kwargs.get('run_id', 'manual_run')
        self.portal_id = kwargs.get('portal_id', 'unknown_portal')

        # Scrapyd passes all arguments as strings, so we must load the JSON
        try:
            self.spider_config = json.loads(config_json) if config_json else {}
        except Exception as e:
            self.logger.error(f"Failed to parse config_json: {e}")
            self.spider_config = {}

    def get_config(self, key, default=None):
        """
        Helper method for child spiders to safely get config values.
        """
        return self.spider_config.get(key, default)

    def build_item(self, response, title, **kwargs):
        """
        Helper to create a standardized LawItem.
        """
        item = JimmyCrawlerItem()

        item['url'] = response.url
        item['portal_id'] = self.portal_id  # Use the ID from kwargs/dashboard
        item['run_id'] = self.run_id
        item['title'] = title.strip() if title else None

        for key, value in kwargs.items():
            if key in item.fields:
                item[key] = value
            else:
                self.logger.warning(f"Field '{key}' is not in JimmyCrawlerItem. Skipping.")

        return item
