import uvicorn
from fastapi import FastAPI, Request

from proxy.config import config

app = FastAPI()


@app.get("/")
async def echo(request: Request):
    return "ok"


@app.post("/echo")
async def echo(request: Request):
    body = await request.body()
    return body.decode("utf-8")

def run_app(host: str, port: int):
    uvicorn.run(app, host=host, port=port, timeout_keep_alive=config.TIMEOUT_KEEP_ALIVE, log_level=config.LOG_LEVEL)
