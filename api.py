# api.py
import asyncio
import json
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse

from Invoice_extractor import (
    pdf_to_base64_images,
    extract_invoice_data
)

app = FastAPI(title="Invoice Extractor API")


async def extract_page_async(page_index: int, img_base64: str):
    """
    Extract a single page in a background thread.
    """
    try:
        raw_json = await asyncio.to_thread(extract_invoice_data, img_base64)
        return f"page_{page_index}", json.loads(raw_json)
    except Exception as e:
        return f"page_{page_index}", {"error": str(e)}


@app.post("/extract-invoice")
async def extract_invoice(file: UploadFile = File(...)):
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="Only PDF files are supported")

    try:
        pdf_bytes = await file.read()

        # Convert PDF → base64 images (blocking → thread)
        pages_base64, _ = await asyncio.to_thread(
            pdf_to_base64_images, pdf_bytes
        )

        if not pages_base64:
            raise HTTPException(status_code=400, detail="No pages found in PDF")

        # Process all pages concurrently
        tasks = [
            extract_page_async(i + 1, img)
            for i, img in enumerate(pages_base64)
        ]

        results = await asyncio.gather(*tasks)

        final_output = {key: value for key, value in results}
        return JSONResponse(content=final_output)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
