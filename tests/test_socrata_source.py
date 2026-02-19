"""Tests for Socrata source plugin with mocked HTTP."""

import responses
import pytest

from parcl.sources.socrata_source import SocrataSource


@responses.activate
def test_socrata_pagination(sample_source_config, sample_crawler_config):
    """Test that SocrataSource paginates and stops on empty page."""
    base = sample_source_config.base_url
    resource = sample_source_config.dataset_id
    url = f"{base}/resource/{resource}.json"

    # Page 1: full batch
    page1 = [{"permit_number": f"P{i}", "status_current": "Issued"} for i in range(10)]
    # Page 2: partial batch (end)
    page2 = [{"permit_number": f"P{i}", "status_current": "Issued"} for i in range(10, 15)]

    responses.add(responses.GET, url, json=page1, status=200)
    responses.add(responses.GET, url, json=page2, status=200)

    source = SocrataSource(sample_source_config, sample_crawler_config)
    batches = list(source.fetch())

    assert len(batches) == 2
    assert len(batches[0]) == 10
    assert len(batches[1]) == 5


@responses.activate
def test_socrata_empty_response(sample_source_config, sample_crawler_config):
    """Test that SocrataSource stops on empty response."""
    base = sample_source_config.base_url
    resource = sample_source_config.dataset_id
    url = f"{base}/resource/{resource}.json"

    responses.add(responses.GET, url, json=[], status=200)

    source = SocrataSource(sample_source_config, sample_crawler_config)
    batches = list(source.fetch())
    assert len(batches) == 0
