# Webapp Makefile for USF Fabric Monitoring Guide

.PHONY: help dev backend frontend install test clean

# Colors
GREEN = \033[0;32m
YELLOW = \033[1;33m
NC = \033[0m

help:
	@echo "$(GREEN)USF Fabric Monitoring - Interactive Guide$(NC)"
	@echo ""
	@echo "$(YELLOW)Development:$(NC)"
	@echo "  make dev       - Start both backend and frontend in development mode"
	@echo "  make backend   - Start backend only (port 8001)"
	@echo "  make frontend  - Start frontend only (port 5173)"
	@echo "  make install   - Install all dependencies"
	@echo ""
	@echo "$(YELLOW)Testing:$(NC)"
	@echo "  make test      - Run all tests"
	@echo "  make test-backend  - Run backend tests only"
	@echo "  make test-frontend - Run frontend tests only"
	@echo ""
	@echo "$(YELLOW)Build:$(NC)"
	@echo "  make build     - Build frontend for production"
	@echo "  make clean     - Clean build artifacts"

install:
	@echo "$(GREEN)Installing backend dependencies...$(NC)"
	cd backend && pip install -r requirements.txt
	@echo "$(GREEN)Installing frontend dependencies...$(NC)"
	cd frontend && npm install

backend:
	@echo "$(GREEN)Starting backend on port 8001...$(NC)"
	cd backend && uvicorn app.main:app --reload --port 8001

frontend:
	@echo "$(GREEN)Starting frontend on port 5173...$(NC)"
	cd frontend && npm run dev

dev:
	@echo "$(GREEN)Starting development servers...$(NC)"
	@echo "Backend: http://localhost:8001"
	@echo "Frontend: http://localhost:5173"
	@make -j2 backend frontend

test: test-backend test-frontend

test-backend:
	@echo "$(GREEN)Running backend tests...$(NC)"
	cd backend && pytest -v

test-frontend:
	@echo "$(GREEN)Running frontend tests...$(NC)"
	cd frontend && npm test

build:
	@echo "$(GREEN)Building frontend for production...$(NC)"
	cd frontend && npm run build

clean:
	@echo "$(GREEN)Cleaning build artifacts...$(NC)"
	rm -rf frontend/dist
	rm -rf backend/__pycache__
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
