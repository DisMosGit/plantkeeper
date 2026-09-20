"""Contract tests for the Trefle wire models.

They pin the exact shapes the adapter consumes and, more importantly, the
permissiveness the feed demands: an extra field must never fail a nightly sync,
and the list payload carries no ``growth`` at all.
"""

from __future__ import annotations

from plantkeeper.infrastructure.external.trefle.models import (
    TrefleDetailResponse,
    TrefleListResponse,
)

LIST_PAYLOAD = {
    "data": [
        {
            "id": 1,
            "slug": "monstera-deliciosa",
            "scientific_name": "Monstera deliciosa",
            "common_name": "Swiss cheese plant",
            "family": "Araceae",
            "genus": "Monstera",
            "image_url": "https://example.invalid/monstera.jpg",
            # Trefle adds fields over time; unknown keys must be ignored.
            "rank": "species",
            "synonyms": [{"name": "Philodendron pertusum"}],
        }
    ],
    "links": {"self": "/api/v1/species?page=1", "next": "/api/v1/species?page=2"},
    "meta": {"total": 417293},
}


def test_a_list_page_parses_and_keeps_the_next_link() -> None:
    page = TrefleListResponse.model_validate(LIST_PAYLOAD)

    assert len(page.data) == 1
    assert page.data[0].slug == "monstera-deliciosa"
    assert page.data[0].common_name == "Swiss cheese plant"
    assert page.links is not None
    assert page.links.next_page == "/api/v1/species?page=2"
    assert page.meta is not None
    assert page.meta.total == 417293


def test_a_list_page_without_links_is_still_valid() -> None:
    page = TrefleListResponse.model_validate({"data": []})

    assert page.links is None
    assert page.meta is None


def test_a_detail_parses_growth() -> None:
    response = TrefleDetailResponse.model_validate(
        {
            "data": {
                "id": 1,
                "slug": "monstera-deliciosa",
                "scientific_name": "Monstera deliciosa",
                "common_name": None,
                "growth": {"light": 5, "soil_humidity": 7, "atmospheric_humidity": 4},
            }
        }
    )

    assert response.data.growth is not None
    assert response.data.growth.light == 5
    assert response.data.growth.soil_humidity == 7


def test_a_detail_without_growth_parses_to_none() -> None:
    response = TrefleDetailResponse.model_validate(
        {
            "data": {
                "id": 1,
                "slug": "monstera-deliciosa",
                "scientific_name": "Monstera deliciosa",
                "common_name": None,
            }
        }
    )

    assert response.data.growth is None
