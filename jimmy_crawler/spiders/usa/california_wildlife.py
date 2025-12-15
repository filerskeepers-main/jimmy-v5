from jimmy_crawler.spiders.base import BaseJimmySpider
from jimmy_crawler.utils import format_date, convert_to_markdown, clean_text


class CaliforniaWildlifeSpider(BaseJimmySpider):
    name = "usa_california_wildlife"
    start_urls = ["https://wildlife.ca.gov/News/Archive"]

    def parse(self, response):
        for article in response.css('.post_article'):
            link = article.css('.la_title h3 a::attr(href)').get()
            if link:
                yield response.follow(link, callback=self.parse_detail)

        next_page = response.css('.pager a.PageNext::attr(href)').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def parse_detail(self, response):
        title = response.css('.la_title h3 a::text').get()

        # The site uses "Month Day, Year" so date_order="MDY" is safer,
        # but "DMY" usually works for written months too.
        raw_date_str = response.css('.article_date::text').get()
        date_obj = format_date(raw_date_str, date_order="MDY")

        # 3. Content - Raw Clean Text
        raw_text_list = response.css('.article_summary ::text').getall()
        clean_content = clean_text(raw_text_list)

        # 4. Content - Markdown
        raw_html = response.css('.article_summary').get()
        markdown_content = convert_to_markdown(raw_html, include_images=True, include_tables=False) # Images included, tables excluded; not tested yet, just for sample

        # 5. Keywords and Tags
        categories = response.css('.article_categories a::text').getall()
        tags = response.css('.article_tags a::text').getall()

        combined_keywords = list(set(categories + tags))

        yield self.build_item(
            response=response,
            title=title,
            date_of_publication=date_obj,
            content=clean_content,
            content_markdown=markdown_content,
            jurisdiction="California",
            legal_classification="legislation",
            source_keywords=combined_keywords,
        )