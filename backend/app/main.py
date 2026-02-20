import sys
import os
os.environ["PYTHONUNBUFFERED"] = "1"


from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks  # ✅ added BackgroundTasks
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware
from app.processor import BulkProcessor
import uuid  # ✅ added

app = FastAPI(title="Fraud Detection Bulk Processor")

# ✅ In-memory job store (added)
jobs = {}

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:5174",
        "https://*.vercel.app",
        "http://127.0.0.1:8000",
        "*"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"message": "Fraud Detection API - Ready"}

# ✅ NEW: background function (same processing logic as before, just moved here)
async def run_excel_job(job_id: str, content: bytes, original_filename: str):
    try:
        processor = BulkProcessor()
        summary, output_bytes = await processor.process_bulk(content)
        jobs[job_id].update({
            "status": "done",
            "result": output_bytes,
            "summary": summary,
        })
        print(f"✅ Job {job_id[:8]} done: {summary}", flush=True)
    except Exception as e:
        print(f"❌ Job {job_id[:8]} failed: {str(e)}", flush=True)
        jobs[job_id].update({
            "status": "failed",
            "error": str(e),
        })

# ✅ CHANGED: now returns job_id immediately instead of waiting 30 mins
@app.post("/api/process-excel")
async def process_excel(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(400, "Only Excel files allowed (.xlsx, .xls)")

    try:
        content = await file.read()
        print(f"\n📁 File received in memory: {file.filename}\n", flush=True)

        job_id = str(uuid.uuid4())
        jobs[job_id] = {
            "status": "processing",
            "result": None,
            "error": None,
            "total": 0,
            "processed": 0,
            "summary": None,
            "filename": file.filename,
        }

        background_tasks.add_task(run_excel_job, job_id, content, file.filename)
        return {"job_id": job_id}  # returns instantly ✅

    except Exception as e:
        print(f"❌ Error: {str(e)}", flush=True)
        raise HTTPException(500, f"Processing failed: {str(e)}")

# ✅ NEW: frontend polls this every 10 seconds
@app.get("/api/jobs/{job_id}")
async def get_job_status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    print(f"[poll] job {job_id[:8]} status={job.get('status')}", flush=True)
    summary = job.get("summary") or {}
    return {
        "status": job["status"],           # processing / done / failed
        "total": job.get("total", 0),
        "processed": job.get("processed", 0),
        "total_rows": summary.get("total_rows", 0),
        "success": summary.get("success", 0),
        "incomplete": summary.get("incomplete", 0),
        "failed": summary.get("failed", 0),
        "error": job.get("error"),
    }

# ✅ NEW: frontend downloads result when status = "done"
@app.get("/api/jobs/{job_id}/download")
async def download_job_result(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    if job["status"] != "done":
        raise HTTPException(400, "Job not ready yet")

    filename = job.get("filename", "result.xlsx")
    base = filename.rsplit(".", 1)[0]
    result_filename = f"{base}_result.xlsx"
    summary = job.get("summary") or {}

    return Response(
        content=job["result"],
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{result_filename}"',
            "X-Total-Rows": str(summary.get("total_rows", 0)),
            "X-Success-Count": str(summary.get("success", 0)),
            "X-Incomplete-Count": str(summary.get("incomplete", 0)),
            "X-Failed-Count": str(summary.get("failed", 0)),
            "Access-Control-Expose-Headers": "X-Total-Rows, X-Success-Count, X-Incomplete-Count, X-Failed-Count, Content-Disposition",
        },
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
