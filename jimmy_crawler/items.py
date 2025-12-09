import scrapy

class JimmyCrawlerItem(scrapy.Item):
    url = scrapy.Field()
    portal_id = scrapy.Field()
    run_id = scrapy.Field()
    jurisdiction = scrapy.Field()

    # --- 2. Content ---
    title = scrapy.Field()
    content = scrapy.Field()          # Raw clean text (no HTML)
    content_markdown = scrapy.Field() # Readable Markdown (with images/tables)
    summary = scrapy.Field()

    # --- 3. Dates ---
    date_of_publication = scrapy.Field()
    date_of_enactment = scrapy.Field()
    date_of_last_amendment = scrapy.Field()
    date_of_effective = scrapy.Field()
    date_of_expiration = scrapy.Field()
    date_of_repealed = scrapy.Field()
    date_of_decision = scrapy.Field()

    # --- 4. Identifiers ---
    document_number = scrapy.Field()
    source_identifier = scrapy.Field()
    source_secondary_identifier = scrapy.Field()

    # --- 5. Legal Metadata ---
    type_of_law = scrapy.Field()
    legal_classification = scrapy.Field()
    issuing_authority = scrapy.Field()
    legal_reference = scrapy.Field()
    is_repealed = scrapy.Field()

    # --- 6. Categorization & Extras ---
    source_keywords = scrapy.Field()
    tags = scrapy.Field()
    extra_metadata = scrapy.Field()

    # --- 7. Files (PDFs/Docs) ---
    file_urls = scrapy.Field()
    file_data = scrapy.Field()
