import asyncio
import json
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from Invoice_extractor import (
    extract_invoice_data,
    transform_to_final_json,
    extract_and_transform,
    extract_all_pages,
    pdf_to_base64_images,
    encode_image
)

app = FastAPI(title="Invoice Extractor API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

import httpx  
import asyncio
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse

# SAP Configuration from your Postman collection
SAP_URL = "https://asd.al-akaria.com/sap/bc/zmiro_post_po?sap-client=110"
SAP_AUTH = ("ohussain", "Skr@@244343P")

def format_sap_payload(final_output: dict):
    """
    Transforms the extraction output to match the SAP JSON schema.
    Removes commas from currency strings as required by SAP.
    """
    return {
        "docDate": final_output["docDate"],
        "postingDate": final_output["postingDate"],
        "refDocno": final_output["refDocno"],
        "companyCode": final_output["companyCode"],
        "currency": final_output["currency"],
        "grossAmount": final_output["grossAmount"].replace(",", ""),
        "item": [
            {
                "invoiceDocItem": item["invoiceDocItem"],
                "poNumber": item.get("poNumber") or "3160000018", 
                "poItem": item["poItem"],
                "quantity": item["quantity"],
                "unit": item["unit"],
                "itemAmount": item["itemAmount"].replace(",", ""),
                "sheetNo": item["sheetNo"]
            }
            for item in final_output["item"]
        ]
    }

async def hit_sap_miro_api(payload: dict):
    """
    Handles the 2-step SAP authentication handshake.
    1. GET call to fetch X-CSRF-Token.
    2. POST call with the token and payload.
    """
    async with httpx.AsyncClient(auth=SAP_AUTH, verify=False) as client:
        try:
            # Step 1: Fetch CSRF Token
            fetch_res = await client.get(SAP_URL, headers={"x-csrf-token": "fetch"})
            csrf_token = fetch_res.headers.get("x-csrf-token")
            
            if not csrf_token:
                return {"error": "Failed to fetch X-CSRF-Token from SAP"}

            # Step 2: POST the formatted data
            headers = {
                "x-csrf-token": csrf_token,
                "Content-Type": "application/json"
            }
            
            sap_res = await client.post(SAP_URL, headers=headers, json=payload)
            return sap_res.json()
        except Exception as e:
            return {"error": f"SAP Connection Failed: {str(e)}"}

@app.post("/extract-and-transform")
async def extract_and_transform_invoice(file: UploadFile = File(...)):
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="Only PDF files are supported")

    try:
        pdf_bytes = await file.read()

        # Layer 1 & 2: Run existing extraction and transformation logic
        result = await asyncio.to_thread(extract_and_transform, pdf_bytes)

        # Trigger SAP Posting if extraction was successful
        if "final_output" in result:
            sap_payload = format_sap_payload(result["final_output"])
            sap_status = await hit_sap_miro_api(sap_payload)
            
            # Append the SAP response to your API output
            result["sap_posting_response"] = sap_status

        return JSONResponse(content=result)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))