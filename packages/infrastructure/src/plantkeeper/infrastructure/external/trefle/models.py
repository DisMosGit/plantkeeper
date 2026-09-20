"""Trefle's wire contract, kept separate from the domain.

These models are the *only* place Trefle's vocabulary is allowed to appear. They
are permissive on purpose: Trefle adds fields over time, and an extra key must
never fail a nightly synchronisation. Everything the project needs is then mapped
to domain value objects in :mod:`plantkeeper.infrastructure.external.trefle.mapping`.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

_TREFLE_CONFIG = ConfigDict(extra="ignore")


class TrefleLinks(BaseModel):
    """The pagination links of a collection response."""

    model_config = _TREFLE_CONFIG

    first: str | None = None
    last: str | None = None
    next_page: str | None = Field(default=None, alias="next")
    prev: str | None = None
    self: str | None = None


class TrefleMeta(BaseModel):
    """The collection metadata."""

    model_config = _TREFLE_CONFIG

    total: int | None = None


class TrefleSpeciesListItem(BaseModel):
    """One entry of a species collection: Trefle's "light" list payload.

    The list endpoints deliberately return taxonomy only; ``growth`` (and with it
    the light and soil indicators) exists only on the detail endpoint, which is
    why the client follows every entry with one detail request.
    """

    model_config = _TREFLE_CONFIG

    id: int
    slug: str
    scientific_name: str
    common_name: str | None = None
    family: str | None = None
    genus: str | None = None
    image_url: str | None = None


class TrefleGrowth(BaseModel):
    """The growing conditions of a species, as ecological indicator classes.

    ``light`` is Ellenberg ``L`` (1 deep shade … 9 full sun) and ``soil_humidity``
    is Ellenberg ``F`` (1 very dry … 12 submerged); both come from the Baseflor
    database and are classes, not measurements. ``None`` means Trefle does not
    know.
    """

    model_config = _TREFLE_CONFIG

    light: int | None = None
    soil_humidity: int | None = None
    atmospheric_humidity: int | None = None


class TrefleSpeciesDetail(TrefleSpeciesListItem):
    """A species as the detail endpoint returns it: the list payload plus growth."""

    growth: TrefleGrowth | None = None


class TrefleListResponse(BaseModel):
    """A page of a Trefle collection."""

    model_config = _TREFLE_CONFIG

    data: list[TrefleSpeciesListItem]
    links: TrefleLinks | None = None
    meta: TrefleMeta | None = None


class TrefleDetailResponse(BaseModel):
    """A single Trefle record."""

    model_config = _TREFLE_CONFIG

    data: TrefleSpeciesDetail
