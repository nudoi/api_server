#
# API Server powered by FastAPI
#
# https://api.env.cs.i.nagoya-u.ac.jp/
#

from fastapi import FastAPI
import api_v1, api_v2
import uvicorn

app = FastAPI(port=58080)


@app.get("/")
def read_root():
    return {"Hello": "World"}

app.include_router(api_v1.router)
app.include_router(api_v2.router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=58080)