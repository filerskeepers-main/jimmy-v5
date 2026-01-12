"""
DUMMY SPIDER: DIRECT_PARTITION MODE
Portal: dummy_direct
Strategy: page_range

This spider demonstrates how to implement a portal that:
- Uses DIRECT_PARTITION mode
- Crawls paginated listings
- Extracts items directly from listing pages
- Handles item deduplication via stable item_key

DO NOT mix this with actual portals like normattiva.
"""

import scrapy
import hashlib
from datetime import datetime
from jimmy_crawler.spiders.base import BaseJimmySpider
from jimmy_crawler.utils import clean_text, format_date


class DummyDirectPartitionSpider(BaseJimmySpider):
    name = "dummy_direct"

    # Portal config (set via dashboard):
    # {
    #   "crawl_mode": "DIRECT_PARTITION",
    #   "partition_strategy": "page_range",
    #   "task_granularity": {
    #     "pages_per_task": 10,
    #     "estimated_duration_minutes": 30
    #   }
    # }

    custom_settings = {
        'LOG_LEVEL': 'INFO',
        'DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 8,
    }

    def get_page_url(self, page: int) -> str:
        """
        Build URL for a specific page number.

        IMPORTANT: This is called by BaseJimmySpider when handling
        page_range partition tasks.

        Args:
            page: Page number from task payload (e.g., 1, 2, 3...)

        Returns:
            Full URL for that page
        """
        # Log what we're crawling
        self.logger.info(f"🔗 Building URL for page {page}")

        # Example: https://dummy-api.com/documents?page=1
        return f"https://dummy-api.com/documents?page={page}"

    def parse(self, response):
        """
        Parse listing page and extract documents.

        This is called for each page in the range.
        Must extract items and yield them.
        """
        page = response.meta.get('page', 1)
        self.logger.info(f"📄 Parsing page {page}")

        # Extract documents from listing
        for doc in response.css('.document-item'):
            # Extract data
            doc_id = doc.css('::attr(data-id)').get()
            title = doc.css('.title::text').get()
            date_str = doc.css('.date::text').get()
            url = doc.css('a::attr(href)').get()

            full_url = response.urljoin(url)

            # CRITICAL: Compute stable item_key for deduplication
            # Option 1: Use document ID
            item_key = f"dummy_{doc_id}"

            # Option 2: Use canonical URL hash
            # canonical_url = self.normalize_url(full_url)
            # item_key = hashlib.sha256(canonical_url.encode()).hexdigest()

            self.logger.info(f"📦 Found document: {title} (key: {item_key})")

            # Yield item
            yield self.build_item(
                response=response,
                title=title,
                url=full_url,
                date_of_publication=format_date(date_str, date_order="MDY"),
                jurisdiction="Dummy Country",
                legal_classification="legislation",
                extra_metadata={
                    'item_key': item_key,
                    'doc_id': doc_id,
                    'page': page
                }
            )

        # Note: No pagination handling here because BaseJimmySpider
        # handles it via task payload (start_page to end_page)

        self.logger.info(f"✅ Completed page {page}")

    def normalize_url(self, url: str) -> str:
        """
        Normalize URL for consistent deduplication.

        Removes:
        - Session tokens
        - Timestamps
        - Tracking parameters

        Sorts:
        - Query parameters alphabetically
        """
        from urllib.parse import urlparse, parse_qs, urlencode

        parsed = urlparse(url)
        query = parse_qs(parsed.query)

        # Remove session/tracking params
        query.pop('session', None)
        query.pop('timestamp', None)
        query.pop('utm_source', None)
        query.pop('utm_medium', None)

        # Sort params
        sorted_query = urlencode(sorted(query.items()))

        # Rebuild
        canonical = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if sorted_query:
            canonical += f"?{sorted_query}"

        return canonical.lower()  # Lowercase for consistency

# Example task payload for this spider:
# {
#   "portal_id": "dummy_direct",
#   "run_id": "run_dummy_abc123",
#   "task_id": "task_xyz789",
#   "mode": "DIRECT_PARTITION",
#   "partition_type": "page_range",
#   "start_page": 1,
#   "end_page": 10
# }

# Expected behavior:
# 1. BaseJimmySpider calls get_page_url(1) through get_page_url(10)
# 2. Each page is requested with callback=self.parse
# 3. parse() extracts items and yields them
# 4. Pipeline upserts items by (portal_id, item_key) unique index
# 5. Task completes when all pages scraped