from fastapi import APIRouter, Request

from api.models import LiveVitals

router = APIRouter()


@router.get("/vitals/live", response_model=list[LiveVitals])
def get_live_vitals(request: Request, ward: str | None = None):
    return request.app.state.repository.latest_live(ward=ward)
