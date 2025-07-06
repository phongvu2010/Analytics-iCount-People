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

# Set all CORS enabled origins
if settings.BACKEND_CORS_ORIGINS:
    origins = [str(origin).strip('/') for origin in settings.BACKEND_CORS_ORIGINS]
    app.add_middleware(
        CORSMiddleware,
        allow_origins = origins if origins else ["*"],  # Cho phép các origin trong danh sách
        allow_credentials = True,       # Cho phép gửi cookie
        allow_methods = ['*'],          # Cho phép tất cả các phương thức (GET, POST, etc.)
        allow_headers = ['*']           # Cho phép tất cả các header
    )

# --- API Routers ---
app.include_router(store.router, prefix = '/api/stores', tags = ['Stores'])
app.include_router(crowd.router, prefix = '/api/crowds', tags = ['Crowds Data'])
app.include_router(error_log.router, prefix = '/api/errors', tags = ['Errors'])

# @app.get('/')
# def read_root():
#     # Trả về trang HTML chính
#     templates = Jinja2Templates(directory='templates')

#     # Logic để render trang ban đầu sẽ ở đây
#     return templates.TemplateResponse('index.html', {'request': {}})


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
