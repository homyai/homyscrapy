import os
import re

from scrapy.http import HtmlResponse, Request

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


def fake_response(filename, url="http://example.com", meta=None):
    """Build a Scrapy HtmlResponse from a fixture file, tied to a Request."""
    path = os.path.join(FIXTURE_DIR, filename)
    with open(path, "rb") as f:
        body = f.read()
    request = Request(url=url, meta=meta or {})
    return HtmlResponse(url=url, body=body, encoding="utf-8", request=request)


def assert_item_schema(item, source, required_fields=None):
    """Assert shared schema invariants on a PropertyItem dict."""
    assert item.get("source") == source, f"source mismatch: {item.get('source')}"
    assert item.get("country") == "Costa Rica"
    assert re.match(r"\d{4}-\d{2}-\d{2}", item.get("extraction_date", ""))
    assert isinstance(item.get("images", []), list)
    assert isinstance(item.get("metadata", {}), dict)
    url = item.get("url", "")
    assert url.startswith("http"), f"url looks invalid: {url}"
    for field in required_fields or []:
        assert item.get(field), f"expected non-empty field: {field}"
