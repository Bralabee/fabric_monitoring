"""
Scenarios API Router

Endpoints for retrieving and navigating scenario content.
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Request

from app.models import Scenario, ScenarioSummary, Category

router = APIRouter()


@router.get("/", response_model=List[ScenarioSummary])
async def list_scenarios(
    request: Request,
    category: Optional[str] = None,
    difficulty: Optional[str] = None,
    tag: Optional[str] = None,
):
    """
    List all available scenarios with optional filtering.
    
    - **category**: Filter by category (e.g., "getting-started", "monitoring")
    - **difficulty**: Filter by difficulty level
    - **tag**: Filter by tag
    """
    scenarios = request.app.state.scenarios
    
    summaries = []
    for scenario in scenarios.values():
        # Apply filters
        if category and scenario.category != category:
            continue
        if difficulty and scenario.difficulty.value != difficulty:
            continue
        if tag and tag not in scenario.tags:
            continue
            
        summaries.append(ScenarioSummary(
            id=scenario.id,
            title=scenario.title,
            description=scenario.description,
            difficulty=scenario.difficulty,
            estimated_duration_minutes=scenario.estimated_duration_minutes,
            tags=scenario.tags,
            category=scenario.category,
            order=scenario.order,
            step_count=len(scenario.steps),
        ))
    
    # Sort by order, then by title
    summaries.sort(key=lambda x: (x.order, x.title))
    return summaries


@router.get("/categories", response_model=List[Category])
async def list_categories(request: Request):
    """
    List all scenario categories with their scenarios.
    """
    scenarios = request.app.state.scenarios
    
    # Define category metadata
    category_meta = {
        "getting-started": {
            "title": "Getting Started",
            "description": "Prerequisites, installation, and initial setup",
            "icon": "rocket",
            "order": 1,
        },
        "monitoring": {
            "title": "Monitoring & Analysis",
            "description": "Monitor Hub extraction and activity analysis",
            "icon": "chart-bar",
            "order": 2,
        },
        "governance": {
            "title": "Governance & Security",
            "description": "Workspace access enforcement and compliance",
            "icon": "shield-check",
            "order": 3,
        },
        "analytics": {
            "title": "Analytics & BI",
            "description": "Star schema building and Power BI integration",
            "icon": "cube",
            "order": 4,
        },
        "deployment": {
            "title": "Fabric Deployment",
            "description": "Deploying to Microsoft Fabric environments",
            "icon": "cloud-arrow-up",
            "order": 5,
        },
        "troubleshooting": {
            "title": "Troubleshooting",
            "description": "Common issues and solutions",
            "icon": "wrench",
            "order": 6,
        },
    }
    
    # Group scenarios by category
    categories_dict = {}
    for scenario in scenarios.values():
        cat = scenario.category
        if cat not in categories_dict:
            meta = category_meta.get(cat, {
                "title": cat.replace("-", " ").title(),
                "description": f"Scenarios for {cat}",
                "icon": "folder",
                "order": 99,
            })
            categories_dict[cat] = {
                "id": cat,
                **meta,
                "scenarios": [],
            }
        
        categories_dict[cat]["scenarios"].append(ScenarioSummary(
            id=scenario.id,
            title=scenario.title,
            description=scenario.description,
            difficulty=scenario.difficulty,
            estimated_duration_minutes=scenario.estimated_duration_minutes,
            tags=scenario.tags,
            category=scenario.category,
            order=scenario.order,
            step_count=len(scenario.steps),
        ))
    
    # Sort scenarios within each category
    for cat in categories_dict.values():
        cat["scenarios"].sort(key=lambda x: (x.order, x.title))
    
    # Convert to list and sort by order
    categories = [Category(**cat) for cat in categories_dict.values()]
    categories.sort(key=lambda x: x.order)
    
    return categories


@router.get("/{scenario_id}", response_model=Scenario)
async def get_scenario(scenario_id: str, request: Request):
    """
    Get a complete scenario with all its steps.
    
    - **scenario_id**: The unique identifier of the scenario
    """
    scenarios = request.app.state.scenarios
    
    if scenario_id not in scenarios:
        raise HTTPException(status_code=404, detail=f"Scenario '{scenario_id}' not found")
    
    return scenarios[scenario_id]


@router.get("/{scenario_id}/steps/{step_id}")
async def get_step(scenario_id: str, step_id: str, request: Request):
    """
    Get a specific step from a scenario.
    
    - **scenario_id**: The unique identifier of the scenario
    - **step_id**: The unique identifier of the step within the scenario
    """
    scenarios = request.app.state.scenarios
    
    if scenario_id not in scenarios:
        raise HTTPException(status_code=404, detail=f"Scenario '{scenario_id}' not found")
    
    scenario = scenarios[scenario_id]
    for step in scenario.steps:
        if step.id == step_id:
            return {
                "scenario": {
                    "id": scenario.id,
                    "title": scenario.title,
                },
                "step": step,
                "step_index": scenario.steps.index(step),
                "total_steps": len(scenario.steps),
                "prev_step": scenario.steps[scenario.steps.index(step) - 1].id if scenario.steps.index(step) > 0 else None,
                "next_step": scenario.steps[scenario.steps.index(step) + 1].id if scenario.steps.index(step) < len(scenario.steps) - 1 else None,
            }
    
    raise HTTPException(status_code=404, detail=f"Step '{step_id}' not found in scenario '{scenario_id}'")
