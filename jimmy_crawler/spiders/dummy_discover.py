"""
DUMMY SPIDER: DISCOVER_THEN_FETCH MODE
Portal: dummy_discover
Strategy: discover

This spider demonstrates how to implement a portal that:
- First discovers all detail page URLs (DISCOVER task)
- Then scrapes each URL in batches (URL_BATCH task)
- Handles metadata collection during discovery
- Deduplicates URLs globally

Use this when you can't partition upfront (unknown total size).

DO NOT mix with actual portals.
"""

import scrapy
import asyncio
import hashlib
from jimmy_crawler.spiders.base import BaseJimmySpider
from jimmy_crawler.utils import clean_text, format_date


class DummyDiscoverSpider(BaseJimmySpider):
    name = "dummy_discover"

    # Portal config (set via dashboard):
    # {
    #   "crawl_mode": "DISCOVER_THEN_FETCH",
    #   "partition_strategy": "discover",
    #   "task_granularity": {
    #     "urls_per_task": 50,
    #     "estimated_duration_minutes": 45
    #   }
    # }

    custom_settings = {
        'LOG_LEVEL': 'INFO',
        'DOWNLOAD_DELAY': 0.3,
        'CONCURRENT_REQUESTS': 16,
    }

    def build_discovery_requests(self, seed: dict):
        """
        Build initial discovery requests.

        This is called by BaseJimmySpider for DISCOVER tasks.

        Args:
            seed: Spider args from run (e.g., start_year, sections, etc.)

        Yields:
            Scrapy Requests to listing pages (not detail pages yet)
        """
        start_year = seed.get('start_year', 2024)
        end_year = seed.get('end_year', 2025)

        self.logger.info(f"🔍 Starting discovery for years {start_year}-{end_year}")

        # Crawl listing pages for each year
        for year in range(start_year, end_year + 1):
            url = f"https://dummy-api.com/archive/{year}"

            self.logger.info(f"📅 Discovering URLs from year {year}")

            yield scrapy.Request(
                url,
                callback=self.parse_for_links,  # Parse to find detail URLs
                meta={'year': year, 'page': 1}
            )

    def parse_for_links(self, response):
        """
        Extract detail page URLs (don't scrape content yet).

        This is the DISCOVERY phase.
        Goal: Collect all URLs, store them, then exit.

        Yields:
            Dicts with 'url' key (BaseJimmySpider stores these)
        """
        year = response.meta['year']
        page = response.meta['page']

        discovered_count = 0
        discovered_urls = []

        # Extract detail URLs
        for link in response.css('a.document-link'):
            url = link.css('::attr(href)').get()
            full_url = response.urljoin(url)

            # Normalize for deduplication
            canonical = self.normalize_url(full_url)

            # Optional: Extract metadata during discovery
            metadata = {
                'title': link.css('::text').get(),
                'section': link.css('::attr(data-section)').get(),
                'year': year,
                'discovered_from': response.url
            }

            discovered_urls.append({
                'url': canonical,
                'metadata': metadata
            })

            # Yield for BaseJimmySpider to collect
            yield {
                'url': canonical,
                'metadata': metadata
            }

            discovered_count += 1

        self.logger.info(f"📦 Discovered {discovered_count} URLs from year {year}, page {page}")

        # Handle pagination in discovery
        next_page = response.css('a.next-page::attr(href)').get()
        if next_page:
            self.logger.info(f"➡️  Following to next page")
            yield response.follow(
                next_page,
                callback=self.parse_for_links,
                meta={'year': year, 'page': page + 1}
            )
        else:
            # End of pagination for this year
            self.logger.info(f"✅ Completed discovery for year {year}")

            # Store discovered URLs in batch
            if discovered_urls:
                # This calls dashboard API to store links
                asyncio.run(self.store_discovered_links_batch(discovered_urls))

    async def store_discovered_links_batch(self, links_with_metadata: list):
        """
        Store discovered URLs with metadata to dashboard.

        Dashboard will:
        1. Deduplicate by url_key
        2. Store in discovered_links collection
        3. Later build URL_BATCH tasks
        """
        import httpx

        urls_only = [link['url'] for link in links_with_metadata]

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.dashboard_url}/api/discovery/links-with-metadata",
                    json={
                        "portal_id": self.portal_id,
                        "run_id": self.run_id,
                        "source_task_id": self.task_id,
                        "links": links_with_metadata
                    },
                    timeout=60.0
                )
                response.raise_for_status()

                result = response.json()
                self.logger.info(f"✅ Stored {result['links_stored']} URLs to dashboard")

        except Exception as e:
            self.logger.error(f"❌ Failed to store URLs: {e}")
            # Don't fail spider, just log

    def parse_detail(self, response):
        """
        Scrape detail page content.

        This is called by BaseJimmySpider for URL_BATCH tasks.
        Now we actually extract content.

        Yields:
            Items with full content
        """
        self.logger.info(f"📄 Scraping detail page: {response.url}")

        # Extract content
        title = response.css('h1.title::text').get()
        date_str = response.css('.publication-date::text').get()
        content_html = response.css('.content').get()
        content_text = clean_text(response.css('.content ::text').getall())

        # Compute item key
        # For detail pages, URL is usually stable
        canonical_url = self.normalize_url(response.url)
        item_key = hashlib.sha256(canonical_url.encode()).hexdigest()

        # Or use document ID if available
        doc_id = response.css('::attr(data-doc-id)').get()
        if doc_id:
            item_key = f"dummy_{doc_id}"

        self.logger.info(f"📦 Extracted: {title} (key: {item_key})")

        # Yield item
        yield self.build_item(
            response=response,
            title=title,
            url=canonical_url,
            date_of_publication=format_date(date_str),
            content=content_text,
            jurisdiction="Dummy Country",
            legal_classification="legislation",
            extra_metadata={
                'item_key': item_key,
                'doc_id': doc_id
            }
        )

    def normalize_url(self, url: str) -> str:
        """Remove session tokens and normalize"""
        from urllib.parse import urlparse, parse_qs, urlencode

        parsed = urlparse(url)
        query = parse_qs(parsed.query)

        # Remove dynamic params
        query.pop('session', None)
        query.pop('_t', None)

        sorted_query = urlencode(sorted(query.items()))
        canonical = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if sorted_query:
            canonical += f"?{sorted_query}"

        return canonical.lower()

# Task flow for this spider:
#
# 1. DISCOVER TASK:
# {
#   "task_type": "DISCOVER",
#   "partition_type": "discover",
#   "seed": {"start_year": 2024, "end_year": 2025}
# }
# → Crawls all listing pages
# → Yields {'url': ...} dicts
# → Calls store_discovered_links_batch()
# → Task completes
#
# 2. Dashboard creates URL_BATCH tasks:
#   POST /api/discovery/build-tasks
#   → Groups discovered URLs into batches of 50
#   → Creates URL_BATCH tasks
#
# 3. URL_BATCH TASK:
# {
#   "task_type": "URL_BATCH",
#   "partition_type": "url_batch",
#   "urls": ["url1", "url2", ..., "url50"]
# }
# → BaseJimmySpider calls parse_detail() for each URL
# → Items extracted and yielded
# → Task completes