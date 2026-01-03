from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

import os
import shutil
import uuid

from master.master import start_job, JOB_STATUS


app = FastAPI(title="Distributed Image Processing")

templates = Jinja2Templates(directory="master/templates")
app.mount("/static", StaticFiles(directory="master/static"), name="static")

UPLOAD_DIR = "data/input"
OUTPUT_DIR = "data/output"

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


# -----------------------------
# UI
# -----------------------------
@app.get("/")
def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request}
    )


# -----------------------------
# API
# -----------------------------
@app.post("/upload")
async def upload_image(
    file: UploadFile = File(...),
    operation: str = "grayscale"
):
    if not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="Invalid image file")

    # SINGLE source of truth for job_id
    job_id = str(uuid.uuid4())

    ext = os.path.splitext(file.filename)[1]
    file_path = os.path.join(UPLOAD_DIR, f"{job_id}{ext}")

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    start_job(file_path, operation, job_id)

    return {
        "job_id": job_id,
        "status": "processing"
    }


@app.get("/status/{job_id}")
def job_status(job_id: str):
    status = JOB_STATUS.get(job_id)

    if status is None:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "job_id": job_id,
        "status": status
    }


@app.get("/result/{job_id}")
def get_result(job_id: str):
    if JOB_STATUS.get(job_id) != "completed":
        raise HTTPException(status_code=400, detail="Job not completed")

    output_path = os.path.join(OUTPUT_DIR, f"{job_id}.png")

    if not os.path.exists(output_path):
        raise HTTPException(status_code=404, detail="Result not found")

    return FileResponse(output_path, media_type="image/png")
