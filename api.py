import asyncio
import json
import os
import base64
from email.mime.text import MIMEText
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from google.auth import default

from Invoice_extractor import (
    extract_invoice_data,
    transform_to_final_json,
    transform_to_sap_retention_json,
    extract_and_transform,
    extract_all_pages,
    pdf_to_base64_images,
    encode_image,
    remove_amount_separators
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
import warnings
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse

# Suppress SSL warnings since SAP APIs use self-signed certificates
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

# ===============================================================================
# SAP WORKFLOW CONFIGURATION
# ===============================================================================
# This system supports multiple SAP workflows with different endpoints and payloads:
# 1. SAP Purchase Order (MIRO) - For invoices with PO
# 2. SAP Retention (F-43) - For invoices without PO (retention cases)
# 
# For each workflow, you need:
# - SAP API endpoint URL
# - Transformation function (in Invoice_extractor.py)
# - Payload formatter function
# ===============================================================================

# SAP Purchase Order Configuration (MIRO API)
SAP_MIRO_URL = "https://asd.al-akaria.com/sap/bc/zmiro_post_po?sap-client=110"
SAP_MIRO_AUTH = ("ohussain", "Skr@@244343P")

# SAP Retention Configuration (F-43 API)
SAP_RETENTION_BASE_URL = "https://asd.al-akaria.com/sap/opu/odata/sap/ZFI_F_43_API_SRV"
SAP_RETENTION_ENTITY = "FHeaderSet"
SAP_RETENTION_CLIENT = "110"
SAP_RETENTION_GET_URL = f"{SAP_RETENTION_BASE_URL}/{SAP_RETENTION_ENTITY}?sap-client={SAP_RETENTION_CLIENT}&$format=json"
SAP_RETENTION_POST_URL = f"{SAP_RETENTION_BASE_URL}/{SAP_RETENTION_ENTITY}?sap-client={SAP_RETENTION_CLIENT}"
SAP_RETENTION_AUTH = ("OHUSSAIN", "Skr@@244343P")

# Legacy compatibility
SAP_URL = SAP_MIRO_URL
SAP_AUTH = SAP_MIRO_AUTH

# Gmail Configuration
GMAIL_SENDER = "muhammadshahzaibb2@gmail.com"
GMAIL_RECIPIENT = "mahmed@al-akaria.com"
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE = "token.json"
SCOPES = ['https://www.googleapis.com/auth/gmail.send']

def send_invoice_success_email(invoice_number: str):
    """
    Send email notification using Google Gmail API (OAuth2).
    Uses credentials.json and token.json for authentication.
    Automatically refreshes expired tokens.
    """
    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
        from google.oauth2.credentials import Credentials
        
        # Load or create credentials
        creds = None
        
        # Load token.json if it exists (JSON format)
        if os.path.exists(TOKEN_FILE):
            try:
                creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
            except Exception as load_error:
                print(f"Error loading token: {load_error}")
                creds = None
        
        # If no valid credentials, refresh or regenerate
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    print("Token expired, refreshing...")
                    creds.refresh(Request())
                    # Save the refreshed credentials as JSON
                    with open(TOKEN_FILE, 'w') as token:
                        token.write(creds.to_json())
                    print("Token refreshed successfully!")
                except Exception as refresh_error:
                    return {"email_status": "failed", "error": f"Token refresh failed: {str(refresh_error)}. Please run generate_token.py"}
            else:
                # Token missing or invalid - check for credentials.json
                if not os.path.exists(CREDENTIALS_FILE):
                    return {"email_status": "failed", "error": f"Missing {CREDENTIALS_FILE}. Download from Google Cloud Console and run generate_token.py"}
                
                # Note: This requires user interaction (browser) - not suitable for production server
                return {"email_status": "failed", "error": "Token missing or invalid. Please run generate_token.py to generate a new token"}
        
        # Build Gmail service
        service = build('gmail', 'v1', credentials=creds)
        
        # Create email message
        message_text = f"Invoice {invoice_number} posted successfully."
        message = MIMEText(message_text)
        message['to'] = GMAIL_RECIPIENT
        message['from'] = GMAIL_SENDER
        message['subject'] = f"Invoice Posted Successfully - {invoice_number}"
        
        # Encode message
        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        send_message = {'raw': raw_message}
        
        # Send the email
        service.users().messages().send(userId='me', body=send_message).execute()
        
        return {"email_status": "sent", "recipient": GMAIL_RECIPIENT}
    except Exception as e:
        return {"email_status": "failed", "error": str(e)}

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
    SAP PURCHASE ORDER (MIRO API) - POST handler
    Handles the 2-step SAP authentication handshake.
    1. GET call to fetch X-CSRF-Token.
    2. POST call with the token and payload.
    """
    async with httpx.AsyncClient(auth=SAP_MIRO_AUTH, verify=False) as client:
        try:
            # Step 1: Fetch CSRF Token
            fetch_res = await client.get(SAP_MIRO_URL, headers={"x-csrf-token": "fetch"})
            csrf_token = fetch_res.headers.get("x-csrf-token")
            
            if not csrf_token:
                return {"error": "Failed to fetch X-CSRF-Token from SAP MIRO"}

            # Step 2: POST the formatted data
            headers = {
                "x-csrf-token": csrf_token,
                "Content-Type": "application/json"
            }
            
            sap_res = await client.post(SAP_MIRO_URL, headers=headers, json=payload)
            return sap_res.json()
        except Exception as e:
            return {"error": f"SAP MIRO Connection Failed: {str(e)}"}


def format_sap_retention_payload(final_output: dict):
    """
    SAP RETENTION (F-43 API) - Payload Formatter
    
    Transforms the extraction output to match the SAP Retention (F-43) JSON schema.
    
    Expected structure:
    {
      "DOC_NO": "1",
      "REF_DOC_NO": "Vendor testing",
      "COMPANY_CODE": "2000",
      "FISCAL_YEAR": "2026",
      "FISCAL_PERIOD": "01",
      "DOCUMENT_DATE": "23.12.2024",
      "DOC_TYPE": "KR",
      "HDRTOITEMNAV": [...]
    }
    """
    return {
        "DOC_NO": final_output.get("DOC_NO", "1"),
        "REF_DOC_NO": final_output.get("REF_DOC_NO", ""),
        "COMPANY_CODE": final_output.get("COMPANY_CODE", "2000"),
        "FISCAL_YEAR": final_output.get("FISCAL_YEAR", "2026"),
        "FISCAL_PERIOD": final_output.get("FISCAL_PERIOD", "01"),
        "DOCUMENT_DATE": final_output.get("DOCUMENT_DATE", ""),
        "DOC_TYPE": final_output.get("DOC_TYPE", "KR"),
        "HDRTOITEMNAV": final_output.get("HDRTOITEMNAV", [])
    }


async def hit_sap_retention_api(payload: dict):
    """
    SAP RETENTION (F-43 API) - POST handler
    
    Handles the 2-step SAP authentication handshake for F-43 API:
    1. GET call to fetch X-CSRF-Token (with $format=json)
    2. POST call with the token and payload
    
    Matches the working logic from test_sap_retention_api.py
    """
    async with httpx.AsyncClient(
        auth=SAP_RETENTION_AUTH, 
        verify=False,
        timeout=30.0
    ) as client:
        try:
            # Step 1: Fetch CSRF Token (using GET with $format=json)
            fetch_res = await client.get(
                SAP_RETENTION_GET_URL,
                headers={
                    "x-csrf-token": "fetch",
                    "Accept": "application/json"
                }
            )
            
            csrf_token = fetch_res.headers.get("x-csrf-token")
            
            if not csrf_token:
                return {
                    "error": "Failed to fetch X-CSRF-Token from SAP Retention API",
                    "status_code": fetch_res.status_code,
                    "response": fetch_res.text
                }

            # Step 2: POST the formatted data
            headers = {
                "x-csrf-token": csrf_token,
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            
            sap_res = await client.post(
                SAP_RETENTION_POST_URL, 
                headers=headers, 
                json=payload
            )
            
            # Handle response based on content type
            content_type = sap_res.headers.get("Content-Type", "")
            
            if "application/json" in content_type:
                response_json = sap_res.json()
                
                # Success detection
                if sap_res.status_code in [200, 201]:
                    return response_json
                else:
                    # SAP returned error in JSON format
                    return {
                        "error": "SAP returned an error",
                        "status_code": sap_res.status_code,
                        "sap_response": response_json
                    }
            else:
                # SAP returned XML error (non-JSON)
                return {
                    "error": "SAP returned non-JSON response",
                    "status_code": sap_res.status_code,
                    "raw_response": sap_res.text
                }
            
        except Exception as e:
            return {
                "error": f"SAP Retention API Connection Failed: {str(e)}",
                "error_type": type(e).__name__
            }

@app.get("/")
async def root():
    """
    API Information - Available Endpoints
    """
    return {
        "api": "Invoice Processing System",
        "version": "2.0",
        "description": "Automatic invoice processing with dual SAP workflow support",
        "endpoints": {
            "/process-invoice": {
                "method": "POST",
                "description": "PRODUCTION - Complete pipeline: Extract → Classify → Transform → Post to SAP → Email",
                "use_case": "Main endpoint for processing invoices in production",
                "workflows": {
                    "sap_po": "Invoices WITH Purchase Order → MIRO API",
                    "sap_retention": "Invoices WITHOUT Purchase Order → F-43 API"
                }
            },
            "/preview-payload": {
                "method": "POST", 
                "description": "TESTING - Generate SAP payload without posting",
                "use_case": "Test and validate transformations before production"
            }
        },
        "features": [
            "Automatic workflow detection (PO vs Retention)",
            "Multi-language support (Arabic + English)",
            "Document type identification (tax_invoice, purchase_order, IPC, etc.)",
            "Email notifications on successful posting",
            "Dual SAP API support (MIRO + F-43)"
        ]
    }


@app.post("/extract-and-transform")
async def process_invoice(file: UploadFile = File(...)):
    """
    UNIFIED INVOICE PROCESSING ENDPOINT - Automatic Workflow Detection & Posting
    
    This is the main production endpoint that handles the complete pipeline:
    
    STEP 1: EXTRACT
    - Extracts all pages from PDF
    - Identifies document types (tax_invoice, purchase_order, IPC, etc.)
    
    STEP 2: CLASSIFY WORKFLOW
    - Has Purchase Order? → SAP PO workflow (MIRO API)
    - No Purchase Order? → SAP Retention workflow (F-43 API)
    
    STEP 3: TRANSFORM
    - SAP PO: Transforms to MIRO format
    - SAP Retention: Transforms to F-43 format
    
    STEP 4: POST TO SAP
    - SAP PO: Posts to MIRO API
    - SAP Retention: Posts to F-43 API
    
    STEP 5: SEND EMAIL
    - Sends notification email if posting successful
    
    The entire process is AUTOMATIC - no manual workflow selection needed!
    """
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="Only PDF files are supported")

    try:
        pdf_bytes = await file.read()

        # Complete pipeline: Extract → Classify → Transform (automatic based on workflow)
        result = await asyncio.to_thread(extract_and_transform, pdf_bytes)
        
        workflow_type = result.get("workflow_type", "sap_po")

        # Post to appropriate SAP API based on detected workflow
        if "final_output" in result:
            if workflow_type == "sap_po":
                # SAP Purchase Order workflow (MIRO API)
                sap_payload = format_sap_payload(result["final_output"])
                sap_status = await hit_sap_miro_api(sap_payload)
                
            elif workflow_type == "sap_retention":
                # SAP Retention workflow (F-43 API)
                sap_payload = format_sap_retention_payload(result["final_output"])
                sap_status = await hit_sap_retention_api(sap_payload)
                
            else:
                raise ValueError(f"Unknown workflow type: {workflow_type}")
            
            # Append the SAP response to output
            result["sap_posting_response"] = sap_status
            
            # Send email ONLY if SAP posting was successful
            if sap_status and "error" not in sap_status:
                # For SAP PO: look for "invoice" field
                # For SAP Retention: look for document number in response (nested in "d" object)
                invoice_number = (
                    sap_status.get("invoice", "").strip() or  # SAP PO
                    sap_status.get("DOC_NO", "").strip() or   # SAP Retention (direct)
                    sap_status.get("d", {}).get("DOC_NO", "").strip()  # SAP Retention (nested)
                )
                
                # Only send email if document number exists and is not empty
                if invoice_number:
                    email_result = send_invoice_success_email(invoice_number)
                    result["email_notification"] = email_result

        return JSONResponse(content=result)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/sap-data")
async def preview_payload(file: UploadFile = File(...)):
    """
    TESTING ENDPOINT - Preview SAP Payload WITHOUT Posting
    
    Does everything that /process-invoice does EXCEPT posting to SAP:
    
    STEP 1: EXTRACT - All pages from PDF
    STEP 2: CLASSIFY - Detect workflow (sap_po or sap_retention)  
    STEP 3: TRANSFORM - Generate SAP payload
    STEP 4: RETURN - Show payload WITHOUT posting to SAP
    
    Use this endpoint to:
    - Verify extraction is correct
    - Check which workflow was detected
    - Validate final payload before production use
    - Debug transformation issues
    
    Perfect for testing before using /process-invoice in production!
    """
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="Only PDF files are supported")

    try:
        pdf_bytes = await file.read()

        # Step 1: Extract raw data with workflow classification
        extracted_data = await asyncio.to_thread(extract_all_pages, pdf_bytes)
        
        workflow_type = extracted_data.get("workflow_type", "sap_po")
        
        # Step 2: Transform to final JSON based on workflow
        if workflow_type == "sap_po":
            final_output = await asyncio.to_thread(transform_to_final_json, extracted_data)
            final_output = remove_amount_separators(final_output)
            sap_payload = format_sap_payload(final_output)
            
        elif workflow_type == "sap_retention":
            # SAP Retention transformation (will use your logic once provided)
            try:
                final_output = await asyncio.to_thread(transform_to_sap_retention_json, extracted_data)
                final_output = remove_amount_separators(final_output)
                sap_payload = format_sap_retention_payload(final_output)
            except NotImplementedError:
                return JSONResponse(content={
                    "status": "info",
                    "workflow_type": workflow_type,
                    "message": "SAP Retention workflow detected. Transformation logic pending.",
                    "extracted_data": extracted_data,
                    "note": "Waiting for transformation logic to be implemented in next prompt."
                })
        else:
            raise ValueError(f"Unknown workflow type: {workflow_type}")

        # Return the SAP payload without posting to SAP
        return JSONResponse(content={
            "status": "success",
            "workflow_type": workflow_type,
            "message": f"{workflow_type.upper()} payload generated (not posted to SAP)",
            "sap_payload": sap_payload,
            "extracted_data": extracted_data,
            "final_output": final_output
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
