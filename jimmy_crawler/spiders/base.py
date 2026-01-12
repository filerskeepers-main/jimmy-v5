import json
import scrapy
import httpx
from typing import Dict, Any, Optional
from datetime import datetime


class BaseJimmySpider(scrapy.Spider):
    """
    Base spider with task payload support for distributed crawling.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Legacy support (manual runs)
        self.portal_id = kwargs.get('portal_id', self.name)
        self.run_id = kwargs.get('run_id', 'manual_run')

        # New: Task-based execution
        self.task_id = kwargs.get('task_id')
        self.task_type = kwargs.get('task_type')

        # Parse task payload if provided
        task_payload_str = kwargs.get('task_payload')
        if task_payload_str:
            try:
                self.task_payload = json.loads(task_payload_str)
                self.logger.info(f"Loaded task payload: {self.task_payload}")
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse task_payload: {e}")
                self.task_payload = {}
        else:
            self.task_payload = {}

        # Dashboard URL for discovery mode
        self.dashboard_url = kwargs.get('dashboard_url', 'http://dashboard-service:8000')

        # Legacy: spider_config (custom args per spider)
        config_json = kwargs.get('config_json', '{}')
        try:
            self.spider_config = json.loads(config_json)
        except:
            self.spider_config = {}

    def start_requests(self):
        """
        Override this in subclass to handle different task types.
        """
        if self.task_payload:
            # Task-based crawling
            yield from self.build_requests_from_task(self.task_payload)
        else:
            # Legacy mode - use spider's default start_urls
            yield from super().start_requests()

    def build_requests_from_task(self, payload: Dict[str, Any]):
        """
        Build Scrapy requests based on task payload.
        Subclasses should override this method.
        """
        partition_type = payload.get('partition_type')
        mode = payload.get('mode')

        if partition_type == 'page_range':
            yield from self._build_page_range_requests(payload)

        elif partition_type == 'date_range':
            yield from self._build_date_range_requests(payload)

        elif partition_type == 'year_range':
            yield from self._build_year_range_requests(payload)

        elif partition_type == 'section':
            yield from self._build_section_requests(payload)

        elif partition_type == 'id_range':
            yield from self._build_id_range_requests(payload)

        elif partition_type == 'alpha_range':
            yield from self._build_alpha_range_requests(payload)

        elif partition_type == 'url_batch':
            yield from self._build_url_batch_requests(payload)

        elif partition_type == 'discover':
            yield from self._build_discover_requests(payload)

        else:
            self.logger.error(f"Unknown partition_type: {partition_type}")

    # ==================== PARTITION HANDLERS ====================

    def _build_page_range_requests(self, payload: Dict[str, Any]):
        """
        Build requests for page range partition.
        Example: Iterate from start_page to end_page.
        """
        start_page = payload.get('start_page', 1)
        end_page = payload.get('end_page', 1)

        for page in range(start_page, end_page + 1):
            url = self.get_page_url(page)
            yield scrapy.Request(
                url,
                callback=self.parse,
                meta={'page': page}
            )

    def _build_date_range_requests(self, payload: Dict[str, Any]):
        """
        Build requests for date range partition.
        Subclass should implement date-specific URL building.
        """
        from_date = payload.get('from_date')
        to_date = payload.get('to_date')

        # Example: single request with date range
        url = self.get_date_range_url(from_date, to_date)
        yield scrapy.Request(url, callback=self.parse)

    def _build_year_range_requests(self, payload: Dict[str, Any]):
        """
        Build requests for year range partition.
        """
        start_year = payload.get('start_year')
        end_year = payload.get('end_year')

        if not start_year or not end_year:
            self.logger.error(f"Missing year range in payload: {payload}")
            return

        self.logger.info(f"Building requests for years {start_year} to {end_year}")

        for year in range(start_year, end_year + 1):
            url = self.get_year_url(year)
            yield scrapy.Request(
                url,
                callback=self.parse_listing if hasattr(self, 'parse_listing') else self.parse,
                meta={'year': year, 'cookiejar': year, 'depth': 1},
                dont_filter=True
            )

    def _build_section_requests(self, payload: Dict[str, Any]):
        """
        Build requests for section partition.
        """
        section_url = payload.get('section_url')
        section_id = payload.get('section_id')

        if section_url:
            yield scrapy.Request(
                section_url,
                callback=self.parse,
                meta={'section_id': section_id}
            )

    def _build_id_range_requests(self, payload: Dict[str, Any]):
        """
        Build requests for ID range partition.
        """
        start_id = payload.get('start_id')
        end_id = payload.get('end_id')

        for doc_id in range(start_id, end_id + 1):
            url = self.get_id_url(doc_id)
            yield scrapy.Request(url, callback=self.parse_detail)

    def _build_alpha_range_requests(self, payload: Dict[str, Any]):
        """
        Build requests for alphabetical range partition.
        """
        from_char = payload.get('from_char')
        to_char = payload.get('to_char')

        # Example: single request covering range
        url = self.get_alpha_range_url(from_char, to_char)
        yield scrapy.Request(url, callback=self.parse)

    def _build_url_batch_requests(self, payload: Dict[str, Any]):
        """
        Build requests for URL batch (DISCOVER_THEN_FETCH mode).
        """
        urls = payload.get('urls', [])

        for url in urls:
            yield scrapy.Request(
                url,
                callback=self.parse_detail,
                errback=self.errback_url
            )

    def _build_discover_requests(self, payload: Dict[str, Any]):
        """
        Build requests for discovery mode.
        """
        seed = payload.get('seed', {})

        # Use seed to build initial discovery requests
        # This should be implemented per spider
        yield from self.build_discovery_requests(seed)

    # ==================== DISCOVERY MODE ====================

    def build_discovery_requests(self, seed: Dict[str, Any]):
        """
        Build initial discovery requests (for DISCOVER mode).
        Override in subclass.
        """
        # Default: use start_urls
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse_for_links)

    def parse_for_links(self, response):
        """
        Parse response to discover detail page URLs.
        Override in subclass to extract links.
        """
        # Example: extract all article links
        for link in response.css('a::attr(href)').getall():
            full_url = response.urljoin(link)
            yield {'url': full_url}

    async def store_discovered_links(self, urls: list):
        """
        Store discovered URLs to the dashboard for later batch processing.
        """
        if not urls:
            return

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.dashboard_url}/api/discovery/links",
                    json={
                        "portal_id": self.portal_id,
                        "run_id": self.run_id,
                        "source_task_id": self.task_id,
                        "urls": urls
                    }
                )
                response.raise_for_status()
                self.logger.info(f"Stored {len(urls)} discovered links")
        except Exception as e:
            self.logger.error(f"Failed to store discovered links: {e}")

    # ==================== HELPER METHODS ====================

    def get_page_url(self, page: int) -> str:
        """
        Build URL for a specific page number.
        Override in subclass.
        """
        raise NotImplementedError("Subclass must implement get_page_url()")

    def get_date_range_url(self, from_date: str, to_date: str) -> str:
        """
        Build URL for date range query.
        Override in subclass.
        """
        raise NotImplementedError("Subclass must implement get_date_range_url()")

    def get_year_url(self, year: int) -> str:
        """
        Build URL for a specific year.
        Override in subclass.
        """
        raise NotImplementedError("Subclass must implement get_year_url()")

    def get_id_url(self, doc_id: int) -> str:
        """
        Build URL for a specific document ID.
        Override in subclass.
        """
        raise NotImplementedError("Subclass must implement get_id_url()")

    def get_alpha_range_url(self, from_char: str, to_char: str) -> str:
        """
        Build URL for alphabetical range.
        Override in subclass.
        """
        raise NotImplementedError("Subclass must implement get_alpha_range_url()")

    def errback_url(self, failure):
        """
        Handle URL fetch errors.
        """
        self.logger.error(f"Request failed: {failure.request.url} - {failure}")

    def get_config(self, key: str, default=None):
        """
        Get configuration value from spider_config.
        """
        return self.spider_config.get(key, default)

    def build_item(self, response, **kwargs):
        """
        Build item with standard fields.
        """
        from jimmy_crawler.items import JimmyCrawlerItem

        item = JimmyCrawlerItem()
        item['url'] = response.url
        item['portal_id'] = self.portal_id
        item['run_id'] = self.run_id

        # Add custom fields
        for key, value in kwargs.items():
            if key in item.fields:
                item[key] = value

        return item