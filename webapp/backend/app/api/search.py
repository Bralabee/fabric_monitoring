"""
Search API Router

Full-text search across scenarios and steps.
"""

from typing import List
from fastapi import APIRouter, Request, Query

from app.models import SearchResult

router = APIRouter()


def calculate_relevance(query: str, text: str, match_type: str) -> float:
    """Calculate relevance score for a search match."""
    query_lower = query.lower()
    text_lower = text.lower()
    
    # Base score by match type
    type_scores = {
        "title": 1.0,
        "tag": 0.9,
        "content": 0.7,
    }
    base_score = type_scores.get(match_type, 0.5)
    
    # Boost for exact matches
    if query_lower == text_lower:
        return base_score * 1.5
    
    # Boost for word start matches
    words = text_lower.split()
    for word in words:
        if word.startswith(query_lower):
            base_score *= 1.2
            break
    
    return min(base_score, 1.0)


def extract_snippet(text: str, query: str, max_length: int = 150) -> str:
    """Extract a snippet around the query match."""
    query_lower = query.lower()
    text_lower = text.lower()
    
    pos = text_lower.find(query_lower)
    if pos == -1:
        return text[:max_length] + "..." if len(text) > max_length else text
    
    # Calculate start and end positions for snippet
    start = max(0, pos - 50)
    end = min(len(text), pos + len(query) + 100)
    
    snippet = text[start:end]
    
    # Add ellipsis if truncated
    if start > 0:
        snippet = "..." + snippet
    if end < len(text):
        snippet = snippet + "..."
    
    return snippet


@router.get("/", response_model=List[SearchResult])
async def search(
    request: Request,
    q: str = Query(..., min_length=2, description="Search query"),
    category: str = Query(None, description="Filter by category"),
    limit: int = Query(20, ge=1, le=100, description="Maximum results"),
):
    """
    Search across all scenarios and steps.
    
    - **q**: Search query (minimum 2 characters)
    - **category**: Optional category filter
    - **limit**: Maximum number of results (default 20)
    """
    scenarios = request.app.state.scenarios
    results = []
    query_lower = q.lower()
    
    for scenario in scenarios.values():
        # Apply category filter
        if category and scenario.category != category:
            continue
        
        # Search in scenario title
        if query_lower in scenario.title.lower():
            results.append(SearchResult(
                scenario_id=scenario.id,
                scenario_title=scenario.title,
                match_type="title",
                snippet=scenario.description,
                relevance_score=calculate_relevance(q, scenario.title, "title"),
            ))
        
        # Search in tags
        for tag in scenario.tags:
            if query_lower in tag.lower():
                results.append(SearchResult(
                    scenario_id=scenario.id,
                    scenario_title=scenario.title,
                    match_type="tag",
                    snippet=f"Tag: {tag} - {scenario.description}",
                    relevance_score=calculate_relevance(q, tag, "tag"),
                ))
                break  # Only one tag match per scenario
        
        # Search in steps
        for step in scenario.steps:
            # Search in step title
            if query_lower in step.title.lower():
                results.append(SearchResult(
                    scenario_id=scenario.id,
                    scenario_title=scenario.title,
                    step_id=step.id,
                    step_title=step.title,
                    match_type="title",
                    snippet=extract_snippet(step.content, q),
                    relevance_score=calculate_relevance(q, step.title, "title"),
                ))
            # Search in step content
            elif query_lower in step.content.lower():
                results.append(SearchResult(
                    scenario_id=scenario.id,
                    scenario_title=scenario.title,
                    step_id=step.id,
                    step_title=step.title,
                    match_type="content",
                    snippet=extract_snippet(step.content, q),
                    relevance_score=calculate_relevance(q, step.content, "content"),
                ))
    
    # Sort by relevance score (descending) and limit results
    results.sort(key=lambda x: x.relevance_score, reverse=True)
    return results[:limit]
