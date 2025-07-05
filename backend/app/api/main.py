from fastapi import APIRouter

# from .routers import data #, logs, auth # Sẽ uncomment sau khi tạo router
# from .routers import store, crowd, error_log

api_router = APIRouter()
# api_router.include_router(login.router, tags = ["Login"])
# api_router.include_router(account.router, prefix = "/account", tags = ["Account"])

# api_router.include_router(auth.router, prefix = '/api/v1', tags = ['Authentication'])
# api_router.include_router(data.router, prefix = '/data', tags = ['Crowd Data'])
# api_router.include_router(logs.router, prefix = '/api/v1', tags = ['Error Logs'])



# app.include_router(store.router)
# app.include_router(crowd.router)
# app.include_router(error_log.router)
