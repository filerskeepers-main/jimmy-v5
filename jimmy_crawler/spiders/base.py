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
        'kwargs' allows you to pass any other field (dates, authority, etc.)
        """
        item = JimmyCrawlerItem()

        # 1. Standard fields explicitly required
        item['url'] = response.url
        item['portal_id'] = self.name
        item['run_id'] = self.run_id
        item['title'] = title.strip() if title else None

        # 2. Fill in optional fields from kwargs
        # This lets you do: self.build_item(..., date_of_enactment="2023-01-01")
        for key, value in kwargs.items():
            if key in item.fields:
                item[key] = value

        # 3. Default for missing important fields
        if 'content_html' not in item:
            item['content_html'] = response.text  # Default to full page

        return item