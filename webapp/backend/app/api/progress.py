"""
Progress API Router

Track user progress through scenarios (session-based, no persistence).
"""

from typing import Dict
from fastapi import APIRouter, HTTPException
from datetime import datetime

from app.models import UserProgress, ProgressUpdate

router = APIRouter()

# In-memory progress storage (session-based)
# In production, this would be stored in a database or localStorage sync
_progress_store: Dict[str, UserProgress] = {}


@router.get("/{scenario_id}", response_model=UserProgress)
async def get_progress(scenario_id: str):
    """
    Get progress for a specific scenario.
    
    - **scenario_id**: The unique identifier of the scenario
    """
    if scenario_id not in _progress_store:
        return UserProgress(scenario_id=scenario_id)
    
    return _progress_store[scenario_id]


@router.post("/{scenario_id}", response_model=UserProgress)
async def update_progress(scenario_id: str, update: ProgressUpdate):
    """
    Update progress for a scenario step.
    
    - **scenario_id**: The unique identifier of the scenario
    - **update**: Progress update with step_id and completion status
    """
    now = datetime.utcnow().isoformat()
    
    if scenario_id not in _progress_store:
        _progress_store[scenario_id] = UserProgress(
            scenario_id=scenario_id,
            started_at=now,
            last_accessed=now,
        )
    
    progress = _progress_store[scenario_id]
    progress.last_accessed = now
    
    if update.completed:
        if update.step_id not in progress.completed_steps:
            progress.completed_steps.append(update.step_id)
    else:
        if update.step_id in progress.completed_steps:
            progress.completed_steps.remove(update.step_id)
    
    return progress


@router.delete("/{scenario_id}")
async def reset_progress(scenario_id: str):
    """
    Reset progress for a scenario.
    
    - **scenario_id**: The unique identifier of the scenario
    """
    if scenario_id in _progress_store:
        del _progress_store[scenario_id]
    
    return {"message": f"Progress reset for scenario '{scenario_id}'"}


@router.get("/")
async def get_all_progress():
    """
    Get progress for all scenarios.
    """
    return list(_progress_store.values())


@router.delete("/")
async def reset_all_progress():
    """
    Reset all progress.
    """
    _progress_store.clear()
    return {"message": "All progress reset"}
