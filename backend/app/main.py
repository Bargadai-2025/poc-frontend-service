from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware
from app.processor import BulkProcessor

app = FastAPI(title="Fraud Detection Bulk Processor")

# ✅ FIXED CORS - Allow frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:5174",
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


@app.post("/api/process-excel")
async def process_excel(file: UploadFile = File(...)):
    """
    Upload Excel → Process in memory → Return result Excel (no files stored).
    """
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(400, "Only Excel files allowed (.xlsx, .xls)")

    try:
        content = await file.read()
        print(f"\n📁 File received in memory: {file.filename}\n")

        processor = BulkProcessor()
        summary, output_bytes = await processor.process_bulk(content)

        # Suggest filename for download (no file saved on server)
        result_filename = "fraud_detection_result.xlsx"
        if file.filename:
            base = file.filename.rsplit(".", 1)[0]
            result_filename = f"{base}_result.xlsx"

        return Response(
            content=output_bytes,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f'attachment; filename="{result_filename}"',
                "X-Total-Rows": str(summary["total_rows"]),
                "X-Success-Count": str(summary["success"]),
                "X-Incomplete-Count": str(summary["incomplete"]),
                "X-Failed-Count": str(summary["failed"]),
                "Access-Control-Expose-Headers": "X-Total-Rows, X-Success-Count, X-Incomplete-Count, X-Failed-Count, Content-Disposition",
            },
        )
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise HTTPException(500, f"Processing failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
