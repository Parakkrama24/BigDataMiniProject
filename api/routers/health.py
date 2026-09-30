from fastapi import APIRouter, Request

from api.models import HealthResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
def health(request: Request):
    repository = request.app.state.repository
    return {
        "status": "ok",
        "database": "ok",
        "last_data_received": repository.last_data_received(),
    }
