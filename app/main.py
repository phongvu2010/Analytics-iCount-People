# Windows: .venv\Scripts\uvicorn.exe app.main:app --host 0.0.0.0 --port 8000 --reload
# Unix: uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path

from .core.config import settings
from .routers import store, crowd, error_log

# --- App Initialization ---
app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.DESCRIPTION
)

# --- Static Files and Templates ---
# This assumes your directory structure is:
# iCount-People-Project/
# ├── app/
# │   ├── main.py
# │   ├── templates/
# │   │   └── index.html
# │   └── static/ (optional, if you have local css/js)
# └── .env
BASE_DIR = Path(__file__).resolve().parent
app.mount('/static', StaticFiles(directory = str(Path(BASE_DIR, 'static'))), name = 'static')
templates = Jinja2Templates(directory=str(Path(BASE_DIR, 'templates')))

# --- CORS Middleware Configuration ---
# FIXED: Ensure CORS is always enabled for development.
# This allows the frontend (even from a different origin) to make API calls to this backend.
origins = []
if settings.BACKEND_CORS_ORIGINS:
    # If the environment variable is set, use it.
    origins.extend([str(origin).strip('/') for origin in settings.BACKEND_CORS_ORIGINS])
else:
    # For local development, allow all origins.
    # In production, you should restrict this to your frontend's domain for security.
    origins = ['*']

app.add_middleware(
    CORSMiddleware,
    allow_origins = origins,    # Cho phép các origin trong danh sách
    allow_credentials = True,   # Cho phép gửi cookie
    allow_methods = ['*'],      # Cho phép tất cả các phương thức (GET, POST, etc.)
    allow_headers = ['*']       # Cho phép tất cả các header
)

# --- API Routers ---
# Note: The trailing slash in the prefix is optional but can help avoid 307 redirects.
# The frontend code has already been updated to include it.
app.include_router(store.router, prefix = '/api/stores', tags = ['Stores'])
app.include_router(crowd.router, prefix = '/api/crowds', tags = ['Crowds Data'])
app.include_router(error_log.router, prefix = '/api/errors', tags = ['Errors'])

# --- Frontend Route ---
@app.get('/', response_class = HTMLResponse, tags = ['Frontend'])
async def read_dashboard(request: Request):
    """ Serves the main dashboard HTML page. """
    return templates.TemplateResponse(
        'index.html',
        {
            'request': request,
            'project_name': settings.PROJECT_NAME
        }
    )

@app.get('/health', tags = ['Root'])
def read_root():
    """ Health check endpoint. """
    return {
        'status': 'ok',
        'message': f'Welcome to {settings.PROJECT_NAME}',
        'sqlalchemy_database_uri': settings.SQLALCHEMY_DATABASE_URI
    }
