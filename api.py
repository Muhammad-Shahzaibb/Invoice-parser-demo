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
            
            # Send email ONLY if SAP posting was successful with a valid invoice number
            if sap_status and "error" not in sap_status:
                invoice_number = sap_status.get("invoice", "").strip()
                # Only send email if invoice number exists and is not empty
                if invoice_number:
                    email_result = send_invoice_success_email(invoice_number)
                    result["email_notification"] = email_result

        return JSONResponse(content=result)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sap-data")
async def sap_data_only(file: UploadFile = File(...)):
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="Only PDF files are supported")

    try:
        pdf_bytes = await file.read()

        # Run extraction + transformation
        result = await asyncio.to_thread(extract_and_transform, pdf_bytes)

        if "final_output" not in result:
            raise HTTPException(status_code=500, detail="Transformation failed")

        # ✅ Return ONLY final_output
        return JSONResponse(content=result["final_output"])

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
