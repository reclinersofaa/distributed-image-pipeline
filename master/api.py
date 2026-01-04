from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from master.worker_metrics import start_worker_metrics, get_worker_snapshot

import os
import shutil
import uuid

from master.master import start_job, JOB_STATUS, JOB_END_TIME, JOB_START_TIME, JOB_TILE_TIMES


app = FastAPI(title="Distributed Image Processing")

@app.on_event("startup")
def startup_event():
    start_worker_metrics()

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

@app.get("/metrics")
def metrics():
    # ---- Worker metrics ----
    workers = get_worker_snapshot()

    total_workers = len(workers)
    busy_workers = sum(1 for w in workers if w["state"] == "busy")
    idle_workers = total_workers - busy_workers

    # ---- Job metrics ----
    active_jobs = sum(
        1 for status in JOB_STATUS.values()
        if status == "processing"
    )

    completed_jobs = sum(
        1 for status in JOB_STATUS.values()
        if status == "completed"
    )

    # ---- Job performance ----
    completed_job_times = [
        JOB_END_TIME[j] - JOB_START_TIME[j]
        for j in JOB_END_TIME
    ]

    avg_job_time = (
        sum(completed_job_times) / len(completed_job_times)
        if completed_job_times else 0
    )

    # ---- Tile performance ----
    all_tile_times = [
        t for times in JOB_TILE_TIMES.values()
        for t in times
    ]

    avg_tile_time_ms = (
        sum(all_tile_times) / len(all_tile_times)
        if all_tile_times else 0
    )

    total_tiles = len(all_tile_times)
    job_durations = [
        JOB_END_TIME[j] - JOB_START_TIME[j]
        for j in JOB_END_TIME
        if j in JOB_START_TIME
    ]

    total_runtime = sum(job_durations)

    tiles_per_sec = (
        total_tiles / total_runtime
        if total_runtime > 0 else 0
    )

    utilization = (
        busy_workers / total_workers
        if total_workers else 0
    )


    return {
        "jobs": {
            "active": active_jobs,
            "completed": completed_jobs
        },
        "workers": {
            "total": total_workers,
            "busy": busy_workers,
            "idle": idle_workers,
            "details": workers
        },
        "performance": {
            "avg_job_time_sec": round(avg_job_time, 2),
            "avg_tile_time_ms": round(avg_tile_time_ms, 2),
            "tiles_per_sec": round(tiles_per_sec, 2)
        },
        "utilization": round(utilization, 2)
    }
