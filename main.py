from fastapi import FastAPI
import api_v1, api_v2


app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}

app.include_router(api_v1.router)
app.include_router(api_v2.router)
