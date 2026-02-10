import scrapy
import asyncio
from jimmy_crawler.spiders.base import BaseJimmySpider
from jimmy_crawler.utils import format_date, convert_to_markdown, clean_text


class CaliforniaWildlifeSpider(BaseJimmySpider):
    name = "usa_california_wildlife"

    # --- MODE A: DISCOVERY (Scanning) ---
    def build_discovery_requests(self, seed: dict):
        # Start at the archive page
        yield scrapy.Request("https://wildlife.ca.gov/News/Archive", callback=self.parse_discovery)

    def parse_discovery(self, response):
        found_links = []
        for article in response.css('.post_article'):
            link = article.css('.la_title h3 a::attr(href)').get()
            if link:
                found_links.append({
                    'url': response.urljoin(link),
                    'metadata': {'source': response.url}
                })

        # Send to Brain
        if found_links:
            asyncio.run(self.store_discovered_links(found_links))

        # Pagination
        next_page = response.css('.pager a.PageNext::attr(href)').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse_discovery)

    # --- MODE B: FETCHING (Scraping) ---
    # This is called automatically by BaseJimmySpider for URL_BATCH tasks
    def parse_detail(self, response):
        title = response.css('.la_title h3 a::text').get()
        date_obj = format_date(response.css('.article_date::text').get(), date_order="MDY")

        clean_content = clean_text(response.css('.article_summary ::text').getall())
        markdown = convert_to_markdown(response.css('.article_summary').get(), include_images=True)

        categories = response.css('.article_categories a::text').getall()
        tags = response.css('.article_tags a::text').getall()

        yield self.build_item(
            response=response,
            title=title,
            date_of_publication=date_obj,
            content=clean_content,
            content_markdown=markdown,
            jurisdiction="California",
            legal_classification="press_release",
            source_keywords=list(set(categories + tags)),
        )
