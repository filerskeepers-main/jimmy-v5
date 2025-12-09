import dateparser
import datetime
from typing import Union
from markdownify import markdownify as md


def format_date(
        date_str: str,
        strict_parsing: bool = True,
        date_order: str = "DMY"
) -> Union[datetime.date, None]:
    """
    Parses a date string into a standard datetime.date object.
    Robust handling for different formats.
    """
    if not isinstance(date_str, str) or not date_str.strip():
        return None

    try:
        dt = dateparser.parse(
            date_str,
            settings={
                "STRICT_PARSING": strict_parsing,
                "PREFER_DAY_OF_MONTH": "first",
                "PREFER_MONTH_OF_YEAR": "first",
                "DATE_ORDER": date_order,
                "RETURN_AS_TIMEZONE_AWARE": False
            }
        )
        return dt.date() if dt else None
    except Exception:
        return None


def convert_to_markdown(
        html_content: str,
        include_images: bool = False,
        include_tables: bool = False
) -> str:
    """
    Converts HTML to Markdown.
    Allows toggling images and tables on/off per portal requirements.
    """
    if not html_content:
        return ""

    # Define what tags to STRIP (remove formatting but keep text)
    # or CONVERT (keep formatting).

    # By default markdownify keeps everything.
    # If we want to exclude images, we strip the <img> tag.
    strip_tags = []
    if not include_images:
        strip_tags.append('img')
    if not include_tables:
        strip_tags.extend(['table', 'thead', 'tbody', 'tr', 'th', 'td'])

    try:
        # heading_style="ATX" makes headers like # Header instead of underlined
        markdown = md(
            html_content,
            heading_style="ATX",
            strip=strip_tags,
            strong_em_symbol="**"  # standard bold
        )

        # Clean up excessive newlines often created by conversion
        cleaned_markdown = markdown.strip()
        while "\n\n\n" in cleaned_markdown:
            cleaned_markdown = cleaned_markdown.replace("\n\n\n", "\n\n")

        return cleaned_markdown
    except Exception as e:
        return html_content  # Fallback if conversion fails


def clean_text(text_list: list) -> str:
    """
    Joins a list of text strings and removes excessive whitespace.
    """
    if not text_list:
        return ""

    # Join with space
    full_text = " ".join(text_list)

    # Remove newlines and tabs, replace multiple spaces with single space
    cleaned = " ".join(full_text.split())
    return cleaned
