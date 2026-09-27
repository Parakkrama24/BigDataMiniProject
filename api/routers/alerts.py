from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request

from api.models import Alert

router = APIRouter()


@router.get("/patients/{patient_id}/alerts", response_model=list[Alert])
def get_patient_alerts(
    patient_id: str,
    request: Request,
    status: Literal["active", "resolved", "all"] = Query("all"),
):
    repository = request.app.state.repository
    if patient_id not in repository.patients:
        raise HTTPException(status_code=404, detail=f"Unknown patient: {patient_id}")
    return repository.patient_alerts(patient_id, status)
