from fastapi import FastAPI, Request

app = FastAPI()


@app.post("/echo")
async def echo(request: Request):
    body = await request.body()
    return body.decode("utf-8")
