# USF Fabric Monitoring - Interactive Guide

An interactive web application that provides step-by-step guidance for using the Microsoft Fabric Monitoring & Governance System.

## Features

- **Chronological Walkthroughs**: Step-by-step guides organized by user scenario
- **Interactive Progress Tracking**: Track your progress through each guide
- **Code Snippets with Copy**: Easily copy commands and code examples
- **Search Functionality**: Find specific topics quickly
- **Dark/Light Mode**: Accessible UI with theme support
- **Mobile Responsive**: Works on all device sizes

## Architecture

```
webapp/
├── backend/           # FastAPI Python backend
│   ├── app/          # Application code
│   │   ├── api/      # API routes
│   │   ├── content/  # YAML scenario definitions
│   │   └── models/   # Pydantic models
│   ├── tests/        # Backend tests
│   └── requirements.txt
├── frontend/         # React TypeScript frontend
│   ├── src/
│   │   ├── components/  # React components
│   │   ├── pages/       # Page components
│   │   ├── hooks/       # Custom hooks
│   │   └── lib/         # Utilities
│   └── package.json
└── docker-compose.yml  # Development orchestration
```

## Quick Start

### Development (Recommended)

```bash
# From webapp directory
make dev
```

This starts both backend (port 8001) and frontend (port 5173).

### Manual Setup

**Backend:**
```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8001
```

**Frontend:**
```bash
cd frontend
npm install
npm run dev
```

## Available Scenarios

1. **Getting Started** - Prerequisites, environment setup, and first steps
2. **Monitor Hub Analysis** - Extract and analyze Fabric activity data
3. **Workspace Access Enforcement** - Ensure security compliance
4. **Star Schema Analytics** - Build dimensional models for BI
5. **Fabric Deployment** - Deploy to Microsoft Fabric notebooks
6. **Troubleshooting** - Common issues and solutions

## API Documentation

Once running, visit http://localhost:8001/docs for the interactive API documentation.

## Technology Stack

- **Backend**: FastAPI, Python 3.11, Pydantic
- **Frontend**: React 18, TypeScript, Vite, Tailwind CSS, shadcn/ui
- **Testing**: pytest (backend), vitest (frontend)
