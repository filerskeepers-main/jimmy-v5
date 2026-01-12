"""
DUMMY SPIDER: HYBRID MODE
Portal: dummy_hybrid
Strategy: section (partitioned discovery)

This spider demonstrates how to implement a portal that:
- Discovers URLs partitioned by section (DISCOVER tasks per section)
- Then scrapes URLs in batches (URL_BATCH tasks)
- Allows parallel discovery across sections
- Handles section-specific metadata

Use this when you have multiple independent sections/categories.

DO NOT mix with actual portals.
"""

import scrapy
import asyncio
import hashlib
from jimmy_crawler.spiders.base import BaseJimmySpider
from jimmy_crawler.utils import clean_text, format_date


class DummyHybridSpider(BaseJimmySpider):
    name = "dummy_hybrid"

    # Portal config (set via dashboard):
    # {
    #   "crawl_mode": "HYBRID",
    #   "partition_strategy": "section",
    #   "task_granularity": {
    #     "urls_per_task": 100,
    #     "estimated_duration_minutes": 60
    #   }
    # }
    #
    # Start run with:
    # {
    #   "portal_id": "dummy_hybrid",
    #   "spider_args": {
    #     "sections": [
    #       {"id": "legislation", "url": "https://dummy.com/legislation"},
    #       {"id": "case_law", "url": "https://dummy.com/case-law"},
    #       {"id": "regulations", "url": "https://dummy.com/regulations"}
    #     ]
    #   }
    # }

    custom_settings = {
        'LOG_LEVEL': 'INFO',
        'DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 12,
    }

    def build_discovery_requests(self, seed: dict):
        """
        Build discovery requests for a specific section.

        In HYBRID mode, this is called PER SECTION.
        Each section gets its own DISCOVER task.

        Args:
            seed: Contains section info from task payload

        Yields:
            Requests to crawl ONE section's listing pages
        """
        # In HYBRID mode with section strategy,
        # the task payload includes section info
        section_id = seed.get('section_id')
        section_url = seed.get('section_url')

        if not section_url:
            self.logger.error("No section_url in seed")
            return

        self.logger.info(f"🔍 Discovering URLs in section: {section_id}")
        self.logger.info(f"🔗 Section URL: {section_url}")

        # Start discovery for this section
        yield scrapy.Request(
            section_url,
            callback=self.parse_for_links,
            meta={'section_id': section_id, 'page': 1}
        )

    def parse_for_links(self, response):
        """
        Discover detail URLs within a section.

        Each DISCOVER task handles one section independently.
        Multiple tasks can run in parallel (one per section).
        """
        section_id = response.meta['section_id']
        page = response.meta['page']

        discovered_urls = []

        # Extract URLs
        for link in response.css('.document-item a'):
            url = link.css('::attr(href)').get()
            full_url = response.urljoin(url)
            canonical = self.normalize_url(full_url)

            # Collect metadata
            metadata = {
                'title': link.css('.title::text').get(),
                'date': link.css('.date::text').get(),
                'section': section_id,
                'type': link.css('::attr(data-type)').get()
            }

            discovered_urls.append({
                'url': canonical,
                'metadata': metadata
            })

            yield {
                'url': canonical,
                'metadata': metadata
            }

        self.logger.info(f"📦 [{section_id}] Page {page}: Discovered {len(discovered_urls)} URLs")

        # Pagination within section
        next_page = response.css('.pagination .next::attr(href)').get()
        if next_page:
            self.logger.info(f"➡️  [{section_id}] Following to page {page + 1}")
            yield response.follow(
                next_page,
                callback=self.parse_for_links,
                meta={'section_id': section_id, 'page': page + 1}
            )
        else:
            # Section complete
            self.logger.info(f"✅ [{section_id}] Discovery complete")

            # Store URLs
            if discovered_urls:
                asyncio.run(self.store_section_urls(section_id, discovered_urls))

    async def store_section_urls(self, section_id: str, links: list):
        """
        Store discovered URLs for a section.

        Links from all sections go to same discovered_links collection,
        but tagged with section_id for tracking.
        """
        import httpx

        self.logger.info(f"💾 [{section_id}] Storing {len(links)} URLs to dashboard")

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.dashboard_url}/api/discovery/links-with-metadata",
                    json={
                        "portal_id": self.portal_id,
                        "run_id": self.run_id,
                        "source_task_id": self.task_id,
                        "links": links
                    },
                    timeout=60.0
                )
                response.raise_for_status()

                result = response.json()
                self.logger.info(f"✅ [{section_id}] Stored {result['links_stored']} URLs")

        except Exception as e:
            self.logger.error(f"❌ [{section_id}] Failed to store URLs: {e}")

    def parse_detail(self, response):
        """
        Scrape detail page (called for URL_BATCH tasks).

        By the time we get here:
        - All DISCOVER tasks completed (one per section)
        - Dashboard created URL_BATCH tasks
        - Worker is executing URL_BATCH task
        """
        self.logger.info(f"📄 Scraping: {response.url}")

        # Extract content
        title = response.css('h1::text').get()
        date_str = response.css('.meta .date::text').get()
        section = response.css('.meta .section::text').get()
        content = clean_text(response.css('.content ::text').getall())

        # Item key
        doc_id = response.css('::attr(data-id)').get()
        if doc_id:
            item_key = f"dummy_{doc_id}"
        else:
            canonical = self.normalize_url(response.url)
            item_key = hashlib.sha256(canonical.encode()).hexdigest()

        self.logger.info(f"📦 Item: {title} (key: {item_key}, section: {section})")

        # Yield item
        yield self.build_item(
            response=response,
            title=title,
            url=response.url,
            date_of_publication=format_date(date_str),
            content=content,
            jurisdiction="Dummy Country",
            legal_classification=section or "unknown",
            extra_metadata={
                'item_key': item_key,
                'doc_id': doc_id,
                'section': section
            }
        )

    def normalize_url(self, url: str) -> str:
        """Normalize URL for deduplication"""
        from urllib.parse import urlparse, parse_qs, urlencode

        parsed = urlparse(url)
        query = parse_qs(parsed.query)

        # Remove session/tracking
        for param in ['session', 'sid', '_t', 'utm_source', 'utm_medium']:
            query.pop(param, None)

        sorted_query = urlencode(sorted(query.items()))
        canonical = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if sorted_query:
            canonical += f"?{sorted_query}"

        return canonical.lower()

# Task flow for HYBRID mode:
#
# 1. Start run creates MULTIPLE DISCOVER tasks (one per section):
#
# Task 1: {
#   "task_type": "DISCOVER",
#   "partition_type": "section",
#   "section_id": "legislation",
#   "section_url": "https://dummy.com/legislation"
# }
#
# Task 2: {
#   "task_type": "DISCOVER",
#   "partition_type": "section",
#   "section_id": "case_law",
#   "section_url": "https://dummy.com/case-law"
# }
#
# Task 3: {
#   "task_type": "DISCOVER",
#   "partition_type": "section",
#   "section_id": "regulations",
#   "section_url": "https://dummy.com/regulations"
# }
#
# → All 3 tasks run IN PARALLEL (different workers)
# → Each discovers URLs for its section
# → All URLs stored in same discovered_links collection
#
# 2. After all DISCOVER tasks complete:
#   POST /api/discovery/build-tasks
#   → Creates URL_BATCH tasks from ALL discovered URLs
#
# 3. URL_BATCH tasks execute:
# {
#   "task_type": "URL_BATCH",
#   "urls": ["url1", "url2", ..., "url100"]
# }
# → parse_detail() scrapes each URL
# → Items yielded
#
# Advantage of HYBRID:
# - Discovery is parallelized (3 workers discovering simultaneously)
# - vs DISCOVER_THEN_FETCH which has 1 discovery task