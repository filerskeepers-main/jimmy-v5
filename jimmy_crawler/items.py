import scrapy


class JimmyCrawlerItem(scrapy.Item):
    # --- 1. Core Identification ---
    url = scrapy.Field()
    portal_id = scrapy.Field()
    run_id = scrapy.Field()
    jurisdiction = scrapy.Field()

    # --- 2. Content ---
    title = scrapy.Field()
    section_title = scrapy.Field()
    summary = scrapy.Field()

    # We keep both. 'content_html' is the raw source. 
    # 'content_markdown' is optional if parsed on the fly.
    content_html = scrapy.Field()
    content_markdown = scrapy.Field()

    # --- 3. Identifiers ---
    document_number = scrapy.Field()
    source_identifier = scrapy.Field()
    source_secondary_identifier = scrapy.Field()

    # --- 4. Dates (Extracted as Strings first) ---
    # We capture them as strings here. The Pipeline will convert them to Dates later.
    date_of_enactment = scrapy.Field()
    date_of_last_amendment = scrapy.Field()
    date_of_publication = scrapy.Field()
    date_of_effective = scrapy.Field()
    date_of_expiration = scrapy.Field()
    date_of_repealed = scrapy.Field()
    date_of_decision = scrapy.Field()

    # --- 5. Legal Metadata ---
    type_of_law = scrapy.Field()
    legal_classification = scrapy.Field()
    issuing_authority = scrapy.Field()
    legal_reference = scrapy.Field()
    is_repealed = scrapy.Field()

    # --- 6. Extra Metadata ---
    tags = scrapy.Field()
    source_keywords = scrapy.Field()
    extra_metadata = scrapy.Field()  # Flexible dict for anything weird

    # --- 7. Files (PDFs) ---
    # 'file_urls' is special in Scrapy. If filled, Scrapy can auto-download them.
    file_urls = scrapy.Field()
    file_data = scrapy.Field()  # To store names, types, or timestamps of files
