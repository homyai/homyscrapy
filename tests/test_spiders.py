"""
Selector smoke tests for static spiders (MLS, RECR).

Uses pre-captured HTML fixtures so tests run without network access.
Run with: pytest tests/test_spiders.py -v
"""

import pytest

from tests.conftest import assert_item_schema, fake_response

# ─── MLS ──────────────────────────────────────────────────────────────────────


class TestMLSSpider:
    @pytest.fixture(autouse=True)
    def spider(self):
        from homyscrapy.spiders.costa_rica.mls import MLSSpider

        self.spider = MLSSpider()

    def test_parse_yields_requests(self):
        response = fake_response("fixture_mls_list.html", url="https://mls.re.cr/search/rs")
        results = list(self.spider.parse(response))
        requests = [r for r in results if hasattr(r, "url")]
        assert len(requests) > 0, "parse() should yield at least one follow request"

    def test_parse_item_fields(self):
        response = fake_response("fixture_mls_list.html", url="https://mls.re.cr/search/rs")
        results = list(self.spider.parse(response))
        # Items are in meta, not yet yielded — check the requests carry partial items
        for r in results:
            if hasattr(r, "meta") and "item" in r.meta:
                item = r.meta["item"]
                assert item.get("source") == "MLS"
                assert item.get("country") == "Costa Rica"
                assert item.get("title")
                break
        else:
            pytest.skip("No item-carrying requests found in fixture")

    def test_parse_property_schema(self):
        list_response = fake_response("fixture_mls_list.html", url="https://mls.re.cr/search/rs")
        requests = [r for r in self.spider.parse(list_response) if hasattr(r, "meta") and "item" in r.meta]
        assert requests, "No detail requests generated"

        # Use the first request's partial item with the detail fixture
        partial_item = requests[0].meta["item"]
        detail_response = fake_response(
            "fixture_mls_detail.html",
            url="https://mls.re.cr/agencies/bluezonerealty/listings/rs2100012",
            meta={"item": partial_item},
        )
        items = list(self.spider.parse_property(detail_response))
        assert items, "parse_property() yielded no items"
        assert_item_schema(items[0], "MLS", required_fields=["title"])

    def test_pagination(self):
        response = fake_response("fixture_mls_list.html", url="https://mls.re.cr/search/rs")
        results = list(self.spider.parse(response))
        # May or may not have a next page — just verify it doesn't crash
        assert results is not None


# ─── RE.CR ────────────────────────────────────────────────────────────────────


class TestRECRSpider:
    @pytest.fixture(autouse=True)
    def spider(self):
        from homyscrapy.spiders.costa_rica.recr import RECRSpider

        self.spider = RECRSpider()

    def test_parse_yields_requests(self):
        response = fake_response(
            "fixture_recr_list.html", url="https://www.re.cr/en/costa-rica-real-estate-for-sale/search-properties"
        )
        results = list(self.spider.parse(response))
        requests = [r for r in results if hasattr(r, "url")]
        assert len(requests) > 0, "parse() should yield at least one follow request"

    def test_parse_item_fields(self):
        response = fake_response(
            "fixture_recr_list.html", url="https://www.re.cr/en/costa-rica-real-estate-for-sale/search-properties"
        )
        for r in self.spider.parse(response):
            if hasattr(r, "meta") and "item" in r.meta:
                item = r.meta["item"]
                assert item.get("source") == "RE.CR"
                assert item.get("country") == "Costa Rica"
                assert item.get("title")
                break
        else:
            pytest.skip("No item-carrying requests found in fixture")

    def test_parse_property_schema(self):
        list_response = fake_response(
            "fixture_recr_list.html", url="https://www.re.cr/en/costa-rica-real-estate-for-sale/search-properties"
        )
        requests = [r for r in self.spider.parse(list_response) if hasattr(r, "meta") and "item" in r.meta]
        assert requests, "No detail requests generated"

        partial_item = requests[0].meta["item"]
        detail_response = fake_response(
            "fixture_recr_detail.html",
            url="https://www.re.cr/en/costa-rica-real-estate-for-sale/search-properties/rs2600291",
            meta={"item": partial_item},
        )
        items = list(self.spider.parse_property(detail_response))
        assert items, "parse_property() yielded no items"
        assert_item_schema(items[0], "RE.CR", required_fields=["title", "description"])


# ─── Pipeline ─────────────────────────────────────────────────────────────────


class TestPipeline:
    def _make_spider(self, name):
        import logging
        import types

        return types.SimpleNamespace(name=name, logger=logging.getLogger(name))

    def test_local_file_created(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

        from datetime import datetime

        from homyscrapy.pipelines import HomyscrapyPipeline

        pipeline = HomyscrapyPipeline()
        spider = self._make_spider("encuentra24")
        # Pin the date so the test is timezone-independent
        spider.output_date = "2026-01-01"
        pipeline.open_spider(spider)

        from homyscrapy.items import PropertyItem

        item = PropertyItem()
        item["source"] = "Encuentra24"
        item["country"] = "Costa Rica"
        item["extraction_date"] = datetime.today().strftime("%Y-%m-%d")
        item["title"] = "Test Property"
        item["url"] = "https://example.com/prop1"
        item["price"] = "$100,000"
        item["images"] = []
        item["metadata"] = {}
        pipeline.process_item(item, spider)

        pipeline.close_spider(spider)

        expected = tmp_path / "data" / "raw" / "encuentra24" / "2026-01-01.json"
        assert expected.exists(), f"Expected output file not found: {expected}"

        import json

        lines = [l for l in expected.read_text().strip().split("\n") if l]
        records = [json.loads(l) for l in lines]
        assert len(records) == 1
        assert records[0]["title"] == "Test Property"

    def test_gcs_not_called_without_credentials(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

        upload_called = []
        monkeypatch.setattr(
            "homyscrapy.pipelines.storage.Client",
            lambda: (_ for _ in ()).throw(AssertionError("GCS should not be called")),
        )

        from datetime import datetime

        from homyscrapy.items import PropertyItem
        from homyscrapy.pipelines import HomyscrapyPipeline

        pipeline = HomyscrapyPipeline()
        spider = self._make_spider("mls")
        pipeline.open_spider(spider)
        item = PropertyItem()
        item["source"] = "MLS"
        item["country"] = "Costa Rica"
        item["extraction_date"] = datetime.today().strftime("%Y-%m-%d")
        item["title"] = "Test"
        item["url"] = "https://example.com"
        item["images"] = []
        item["metadata"] = {}
        pipeline.process_item(item, spider)
        pipeline.close_spider(spider)

        assert not upload_called, "GCS upload should not be called without credentials"


# ─── google_cloud_tools ───────────────────────────────────────────────────────


class TestGoogleCloudTools:
    def test_strip_accents(self):
        from homyscrapy.common.google_cloud_tools.google_cloud_tools import strip_accents

        assert strip_accents("café") == "cafe"
        assert strip_accents("Número") == "Numero"

    def test_text_to_id_basic(self):
        from homyscrapy.common.google_cloud_tools.google_cloud_tools import text_to_id

        assert text_to_id("Total Lot Size") == "total_lot_size"
        assert text_to_id("listing_id") == "listing_id"

    def test_text_to_id_leading_digit(self):
        from homyscrapy.common.google_cloud_tools.google_cloud_tools import text_to_id

        result = text_to_id("1st_floor")
        assert result.startswith("_"), f"Expected leading underscore, got: {result}"

    def test_text_to_id_no_strip_ones(self):
        # Regression: old code did re.sub("1", "", text) which corrupted names containing "1"
        from homyscrapy.common.google_cloud_tools.google_cloud_tools import text_to_id

        result = text_to_id("listing_id")
        assert "1" not in result or "listing_id" in result, f"Unexpected result: {result}"
        # More direct: a field that had a "1" in the middle should keep it (unless leading digit)
        result2 = text_to_id("floor_1")
        assert result2 == "floor_1", f"Expected 'floor_1', got '{result2}'"

    def test_date_manager_format(self):
        from homyscrapy.common.google_cloud_tools.google_cloud_tools import date_manager

        result = date_manager()
        assert len(result) == 8
        assert result.isdigit()
