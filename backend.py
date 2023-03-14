from fastapi import FastAPI, File, UploadFile, Request
import shutil
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import Optional

import ray

app = FastAPI()
templates = Jinja2Templates(directory="templates")

#https://gist.github.com/architkulkarni/71c856c5d63bf772bf83e2e7744d11a2
@app.on_event("startup") # Code to be run when the server starts.
async def startup_event():
    ray.init(address="auto") # Connect to the running Ray cluster.

@app.get("/upload/", response_class=HTMLResponse)
async def upload(request: Request):
   return templates.    TemplateResponse("uploadfile.html", {"request": request})

@app.post("/uploader/")
async def create_upload_file(file: UploadFile = File(...)):
   with open("destination.png", "wb") as buffer:
      shutil.copyfileobj(file.file, buffer)
   return {"filename": file.filename}

@app.post("/search/")
async def search(request: Request, query: Optional[str] = None):
    return ""


#https://fastapi.tiangolo.com/advanced/websockets/