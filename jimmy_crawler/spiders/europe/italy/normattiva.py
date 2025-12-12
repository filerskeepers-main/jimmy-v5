import scrapy
import datetime
import math
import re
from urllib.parse import urlparse, parse_qs, urlencode
from jimmy_crawler.spiders.base import BaseJimmySpider
from jimmy_crawler.utils import clean_text, convert_to_markdown, format_date


class ItalyNormattivaHttpSpider(BaseJimmySpider):
    name = "normattiva"  # Ensure this matches what you run in CLI

    EXPORT_URL = "https://www.normattiva.it/esporta/attoCompleto"

    custom_settings = {
        # 1. Throttling
        'DOWNLOAD_DELAY': 1.5,
        'CONCURRENT_REQUESTS': 16,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 8,

        # 2. Cookies & Session
        'COOKIES_ENABLED': True,
        'COOKIES_DEBUG': False,

        # 3. Headers (REMOVED REFERER to let Scrapy handle it automatically)
        'DEFAULT_REQUEST_HEADERS': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
            # 'Referer': ...  <-- DELETED. Scrapy will auto-fill this, keeping the session valid.
        },

        'RETRY_ENABLED': True,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],
    }

    def start_requests(self):
        current_year = datetime.datetime.now().year
        # start_year = 1861 # Use this for full crawl
        start_year = 2025  # Testing with 2025

        for year in range(start_year, current_year + 1):
            year_str = str(year)
            url = f"https://www.normattiva.it/ricerca/elencoPerData/anno/{year_str}"

            yield scrapy.Request(
                url,
                callback=self.parse_listing,
                meta={
                    'cookiejar': year,  # Separate session per year
                    'year': year_str,
                    'is_start_page': True  # Explicit flag to trigger pagination logic
                },
                dont_filter=True
            )

    def parse_listing(self, response):
        """
        Step 1: Parse the Listing Page & Handle Pagination
        """
        # --- 0. Fix the Crash (Safe Page Detection) ---
        active_page_text = response.css('li.page-item.active a::text').get()
        # Safe conversion: check if text exists AND is a digit
        if active_page_text and active_page_text.strip().isdigit():
            active_page = int(active_page_text.strip())
        else:
            active_page = "Unknown"

        # --- 1. Extract Links ---
        detail_links = response.xpath("//a[contains(@href, 'caricaDettaglioAtto')]")
        act_count = len(detail_links)

        self.logger.info(
            f"[YEAR {response.meta['year']}] URL: {response.url} | Server Page: {active_page} | Acts: {act_count}")

        # Stop if no data
        if act_count == 0:
            return

        for link_node in detail_links:
            # ... (Your existing extraction code here) ...
            # Keep the exact code you had for 'raw_title', 'relative_url', 'pub_date', etc.
            # ...
            # (Just hiding it here to keep the answer short, but keep it in your file!)
            raw_title = link_node.xpath("./text() | ./parent::*/text()").get()
            title = clean_text([raw_title]) if raw_title else "Untitled Act"

            relative_url = link_node.xpath("./@href").get()
            parsed_url = urlparse(relative_url)
            query_params = parse_qs(parsed_url.query)

            pub_date = query_params.get('atto.dataPubblicazioneGazzetta', [None])[0]
            code_red = query_params.get('atto.codiceRedazionale', [None])[0]

            if pub_date and code_red:
                export_params = {
                    'atto.dataPubblicazioneGazzetta': pub_date,
                    'atto.codiceRedazionale': code_red
                }
                full_export_url = f"{self.EXPORT_URL}?{urlencode(export_params)}"

                yield scrapy.Request(
                    full_export_url,
                    callback=self.parse_full_text,
                    meta={
                        'title': title,
                        'pub_date': pub_date,
                        'doc_code': code_red,
                        'cookiejar': response.meta['cookiejar']
                    }
                )

        # --- 2. PAGINATION (The Critical Fix) ---

        # A. Extract the tabID from the CURRENT URL
        # URL looks like: .../anno/2025?tabID=0.5566...
        current_parsed = urlparse(response.url)
        current_qs = parse_qs(current_parsed.query)

        # Get tabID from URL, or fallback to meta if we saved it before
        tab_id = current_qs.get('tabID', [None])[0]
        if not tab_id:
            tab_id = response.meta.get('tabID')

        # B. Calculate Indexes
        current_page_index = 0
        if "/anno/" in response.url:
            current_page_index = 0
        else:
            match = re.search(r'/elencoPerData/(\d+)', response.url)
            if match:
                current_page_index = int(match.group(1))

        next_page_index = current_page_index + 1

        # C. Check if "Next" exists in the HTML
        expected_next_href = f"/ricerca/elencoPerData/{next_page_index}"
        next_link_exists = response.xpath(f"//a[contains(@href, '{expected_next_href}')]")

        if next_link_exists:
            # Construct base URL
            base_next_url = f"https://www.normattiva.it{expected_next_href}"

            # CRITICAL: Append the tabID if we found one
            if tab_id:
                final_next_url = f"{base_next_url}?tabID={tab_id}"
            else:
                final_next_url = base_next_url

            self.logger.info(f"[YEAR {response.meta['year']}] Moving to Page Index {next_page_index} (TabID: {tab_id})")

            yield scrapy.Request(
                final_next_url,
                callback=self.parse_listing,
                meta={
                    'cookiejar': response.meta['cookiejar'],
                    'year': response.meta['year'],
                    'tabID': tab_id  # Pass it forward for future pages
                },
                dont_filter=True,
                headers={'Referer': response.url}
            )
        else:
            self.logger.info(f"[YEAR {response.meta['year']}] Reached last page (Index {current_page_index}).")

    def parse_full_text(self, response):
        # ... (Same as before) ...
        raw_html = response.css('body').get()
        content_lines = response.css('body ::text').getall()
        clean_content = clean_text(content_lines)
        markdown_content = convert_to_markdown(raw_html, include_tables=True)

        effective_date_str = response.xpath("//span[contains(text(), 'vigore')]/following-sibling::span/text()").get()
        effective_date_obj = format_date(effective_date_str) if effective_date_str else None
        pub_date_obj = format_date(response.meta['pub_date'], date_order="YMD")

        yield self.build_item(
            response=response,
            title=response.meta['title'],
            jurisdiction="Italy",
            source_identifier=response.meta['doc_code'],
            date_of_publication=pub_date_obj,
            date_of_effective=effective_date_obj,
            content=clean_content,
            content_markdown=markdown_content,
            extra_metadata={
                "source_url": response.url,
                "is_export_version": True
            }
        )