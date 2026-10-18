from typing import Any, Dict
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from src.services.cache_service import CacheService
from src.services.metrics import metrics

router = APIRouter()


class MetricsResponse(BaseModel):
    """Schema matching the exact metrics contract."""
    l1_hits: int
    l2_hits: int
    cache_misses: int
    stampedes_prevented: int
    l1_evictions: int


class DataResponse(BaseModel):
    """Data response containing key and payload."""
    id: str
    key: str
    data: str


# Global dependency provider placeholder (injected in main)
_cache_service: CacheService = None  # type: ignore


def set_cache_service(service: CacheService) -> None:
    global _cache_service
    _cache_service = service


def get_cache_service() -> CacheService:
    if _cache_service is None:
        raise RuntimeError("CacheService is not initialized")
    return _cache_service


@router.get("/data/{key}", response_model=DataResponse)
async def get_data_endpoint(
    key: str,
    bypass_cache: bool = Query(default=False, description="If true, bypasses caches and fetches from Oracle"),
    cache_service: CacheService = Depends(get_cache_service),
) -> DataResponse:
    """
    Primary Data Interface:
    GET /data/{key}
    Optional query parameter: ?bypass_cache=true
    """
    data = await cache_service.get_data(key=key, bypass_cache=bypass_cache)
    return DataResponse(id=key, key=key, data=data)


@router.get("/metrics", response_model=MetricsResponse)
async def get_metrics_endpoint() -> MetricsResponse:
    """
    Metrics Interface:
    GET /metrics
    Returns real-time performance statistics.
    """
    raw_metrics = metrics.get_metrics()
    return MetricsResponse(
        l1_hits=raw_metrics["l1_hits"],
        l2_hits=raw_metrics["l2_hits"],
        cache_misses=raw_metrics["cache_misses"],
        stampedes_prevented=raw_metrics["stampedes_prevented"],
        l1_evictions=raw_metrics["l1_evictions"],
    )
