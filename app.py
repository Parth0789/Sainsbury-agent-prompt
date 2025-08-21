from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import router
from route.authorization import router as auth_router
from route.support_tickets import router as support_tickets_router
from route.status import router as cam_sys_status_router
from route.reports import router as report_router
from route.transactions import router as transaction_router
from route.stores import router as stores_router
from route.media import router as media_router

app = FastAPI()
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router=router)

app.include_router(router=auth_router)
app.include_router(router=support_tickets_router)
app.include_router(router=cam_sys_status_router)
app.include_router(router=report_router)
app.include_router(router=transaction_router)
app.include_router(router=stores_router)
app.include_router(media_router)