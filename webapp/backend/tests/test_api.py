"""
Tests for the Guide API
"""

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.content.loader import load_all_scenarios


@pytest.fixture(scope="module")
def client():
    """Create a test client with scenarios loaded."""
    # Manually load scenarios for testing (lifespan doesn't run in TestClient by default)
    app.state.scenarios = load_all_scenarios()
    with TestClient(app) as test_client:
        yield test_client


class TestHealthEndpoints:
    """Test health check endpoints."""
    
    def test_root_endpoint(self, client):
        """Test root endpoint returns welcome message."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "version" in data
        assert data["status"] == "healthy"
    
    def test_health_check(self, client):
        """Test health check endpoint."""
        response = client.get("/api/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"


class TestScenariosAPI:
    """Test scenarios API endpoints."""
    
    def test_list_scenarios(self, client):
        """Test listing all scenarios."""
        response = client.get("/api/scenarios/")
        assert response.status_code == 200
        scenarios = response.json()
        assert isinstance(scenarios, list)
        assert len(scenarios) > 0
    
    def test_list_categories(self, client):
        """Test listing scenario categories."""
        response = client.get("/api/scenarios/categories")
        assert response.status_code == 200
        categories = response.json()
        assert isinstance(categories, list)
        # Should have categories like getting-started, monitoring, etc.
        category_ids = [c["id"] for c in categories]
        assert "getting-started" in category_ids
    
    def test_get_scenario(self, client):
        """Test getting a specific scenario."""
        response = client.get("/api/scenarios/getting-started")
        assert response.status_code == 200
        scenario = response.json()
        assert scenario["id"] == "getting-started"
        assert "title" in scenario
        assert "steps" in scenario
        assert len(scenario["steps"]) > 0
    
    def test_get_nonexistent_scenario(self, client):
        """Test getting a scenario that doesn't exist."""
        response = client.get("/api/scenarios/nonexistent-scenario")
        assert response.status_code == 404
    
    def test_scenario_structure(self, client):
        """Test that scenarios have the expected structure."""
        response = client.get("/api/scenarios/getting-started")
        scenario = response.json()
        
        # Required fields
        assert "id" in scenario
        assert "title" in scenario
        assert "description" in scenario
        assert "difficulty" in scenario
        assert "estimated_duration_minutes" in scenario
        assert "steps" in scenario
        
        # Steps should have required fields
        for step in scenario["steps"]:
            assert "id" in step
            assert "title" in step
            assert "type" in step
            assert "content" in step
    
    def test_filter_by_category(self, client):
        """Test filtering scenarios by category."""
        response = client.get("/api/scenarios/?category=getting-started")
        assert response.status_code == 200
        scenarios = response.json()
        for scenario in scenarios:
            assert scenario["category"] == "getting-started"
    
    def test_filter_by_difficulty(self, client):
        """Test filtering scenarios by difficulty."""
        response = client.get("/api/scenarios/?difficulty=beginner")
        assert response.status_code == 200
        scenarios = response.json()
        for scenario in scenarios:
            assert scenario["difficulty"] == "beginner"


class TestSearchAPI:
    """Test search API endpoints."""
    
    def test_search_basic(self, client):
        """Test basic search functionality."""
        response = client.get("/api/search/?q=monitor")
        assert response.status_code == 200
        results = response.json()
        assert isinstance(results, list)
    
    def test_search_with_limit(self, client):
        """Test search with result limit."""
        response = client.get("/api/search/?q=fabric&limit=5")
        assert response.status_code == 200
        results = response.json()
        assert len(results) <= 5
    
    def test_search_minimum_length(self, client):
        """Test that search requires minimum query length."""
        response = client.get("/api/search/?q=a")
        assert response.status_code == 422  # Validation error
    
    def test_search_result_structure(self, client):
        """Test that search results have expected structure."""
        response = client.get("/api/search/?q=environment")
        results = response.json()
        
        for result in results:
            assert "scenario_id" in result
            assert "scenario_title" in result
            assert "match_type" in result
            assert "snippet" in result
            assert "relevance_score" in result


class TestProgressAPI:
    """Test progress tracking API endpoints."""
    
    def test_get_empty_progress(self, client):
        """Test getting progress for a scenario with no progress."""
        response = client.get("/api/progress/getting-started")
        assert response.status_code == 200
        progress = response.json()
        assert progress["scenario_id"] == "getting-started"
        assert progress["completed_steps"] == []
        assert progress["completed"] == False
    
    def test_update_progress(self, client):
        """Test updating progress for a step."""
        # Update progress
        response = client.post(
            "/api/progress/getting-started",
            json={"step_id": "overview", "completed": True}
        )
        assert response.status_code == 200
        progress = response.json()
        assert "overview" in progress["completed_steps"]
        
        # Verify progress persists
        response = client.get("/api/progress/getting-started")
        progress = response.json()
        assert "overview" in progress["completed_steps"]
    
    def test_reset_progress(self, client):
        """Test resetting progress for a scenario."""
        # First set some progress
        client.post(
            "/api/progress/test-scenario",
            json={"step_id": "step1", "completed": True}
        )
        
        # Reset it
        response = client.delete("/api/progress/test-scenario")
        assert response.status_code == 200
        
        # Verify it's reset
        response = client.get("/api/progress/test-scenario")
        progress = response.json()
        assert progress["completed_steps"] == []
    
    def test_get_all_progress(self, client):
        """Test getting all progress entries."""
        response = client.get("/api/progress/")
        assert response.status_code == 200
        assert isinstance(response.json(), list)


class TestContentLoading:
    """Test content loading functionality."""
    
    def test_all_scenarios_load(self, client):
        """Test that all expected scenarios are loaded."""
        response = client.get("/api/scenarios/")
        scenarios = response.json()
        
        expected_ids = [
            "getting-started",
            "monitor-hub-analysis",
            "workspace-access-enforcement",
            "star-schema-analytics",
            "fabric-deployment",
            "troubleshooting"
        ]
        
        loaded_ids = [s["id"] for s in scenarios]
        for expected_id in expected_ids:
            assert expected_id in loaded_ids, f"Missing scenario: {expected_id}"
    
    def test_scenario_order(self, client):
        """Test that scenarios are returned in correct order."""
        response = client.get("/api/scenarios/")
        scenarios = response.json()
        
        # Verify order by checking the order field is ascending
        orders = [s["order"] for s in scenarios]
        assert orders == sorted(orders)
