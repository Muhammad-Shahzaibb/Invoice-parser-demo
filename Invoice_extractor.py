import os
import base64
import json
import fitz  # PyMuPDF
import io
import logging
import random
from dotenv import load_dotenv
from groq import Groq
from huggingface_hub import InferenceClient

# -------------------------
# Setup logging
# -------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------
# Load env
# -------------------------
load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
HF_TOKEN = os.getenv("HF_TOKEN") # Ensure this is in your .env

if not GROQ_API_KEY:
    raise ValueError("GROQ_API_KEY not found in .env file!")
if not HF_TOKEN:
    raise ValueError("HF_TOKEN not found in .env file!")

groq_client = Groq(api_key=GROQ_API_KEY)
# Using the Inference Client instead of local model
hf_client = InferenceClient(api_key=HF_TOKEN)

MAX_PAGES = int(os.getenv("MAX_PAGES", 5))

# -------------------------
# Helpers
# -------------------------
def encode_image(image_bytes):
    return base64.b64encode(image_bytes).decode("utf-8")

def extract_invoice_data(image_bytes):
    """
    Extract data from a single page image using Hugging Face Inference API (Qwen3-VL).
    """
    prompt = """You are an expert invoice OCR and document understanding AI.
Extract all visible information from this image and categorize it by document type.
The document may contain Arabic, English, or both.

FIRST: Identify what type of document this page contains:
- "tax_invoice" - Tax Invoice / Invoice / فاتورة ضريبية (contains invoice number / رقم الفاتورة, seller/buyer info, line items with prices, VAT, totals)
- "purchase_order" - Purchase Order / PO / أمر الشراء (contains PO number, vendor info, ordered items)
- "gl_document" - GL Document / Accounting Document / مستند محاسبي (contains posting entries, account numbers, debit/credit)

REQUIRED OUTPUT FORMAT:
{
  "document_type": "tax_invoice" | "purchase_order" | "gl_document",
  "data": {
    // All extracted data from this page goes here
  }
}

RULES:
- Identify the document type based on the content
- Capture ALL data visible on the page inside "data" object
- Do not invent or translate values
- Preserve original text exactly (Arabic and English)
- Return ONE valid JSON object only
- No explanations, no markdown

Return the JSON object only."""

    try:
        # Convert image to base64 for API transmission
        base64_image = encode_image(image_bytes)
        
        # Call the HF Inference API

        response_text = ""
        for message in hf_client.chat_completion(
            # model="Qwen/Qwen3-VL-8B-Instruct",
            model="Qwen/Qwen3-VL-32B-Instruct", 
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"},
                        },
                    ],
                }
            ],
            max_tokens=4096,
            stream=False,
        ).choices:
            response_text = message.message.content

        # Clean response and extract JSON
        response = response_text.strip()
        if response.startswith("```json"):
            response = response[7:]
        elif response.startswith("```"):
            response = response[3:]
        if response.endswith("```"):
            response = response[:-3]
        response = response.strip()
        
        # Validate JSON
        json.loads(response)
        return response
        
    except Exception as e:
        logger.error(f"HF API extraction failed: {e}")
        raise

def pdf_to_images(pdf_bytes):
    """Convert PDF bytes to image bytes (up to MAX_PAGES)."""
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:
        logger.error(f"Failed to open PDF: {e}")
        raise

    page_count = min(len(doc), MAX_PAGES)
    pages_images = []

    for page_index in range(page_count):
        try:
            page = doc.load_page(page_index)
            pix = page.get_pixmap(matrix=fitz.Matrix(1.5, 1.5))
            img_data = pix.tobytes("jpg")
            pages_images.append(img_data)
        except Exception as e:
            logger.error(f"Failed to process page {page_index+1}: {e}")

    doc.close()
    return pages_images


def pdf_to_base64_images(pdf_bytes):
    """
    Convert PDF bytes to base64 images (backward compatibility).
    Returns: (list of base64 strings, list of image bytes)
    """
    pages_images = pdf_to_images(pdf_bytes)
    pages_base64 = [encode_image(img) for img in pages_images]
    return pages_base64, pages_images


def transform_to_final_json(extracted_data: dict) -> dict:
    """
    Process extracted JSON through Groq LLM to generate
    the final structured output with required fields.
    Uses llama-3.3-70b-versatile model for fast processing.
    """
    import random
    
    prompt = f"""You are a data transformation expert. Analyze the following extracted invoice/purchase order data and transform it into the exact JSON structure required.

EXTRACTED DATA:
{json.dumps(extracted_data, indent=2, ensure_ascii=False)}

REQUIRED OUTPUT FORMAT:
{{
  "docDate": "YYYYMMDD",  // Document date in YYYYMMDD format
  "postingDate": "YYYYMMDD",  // Posting date in YYYYMMDD format (use today if not found)
  "refDocno": "",  // Invoice Number
  "companyCode": "2000",  // Always fixed as "2000"
  "currency": "SAR",  // Always fixed as "SAR"
  "grossAmount": "",  // Total amount from tax invoice (with VAT)
  "item": [
    {{
      "invoiceDocItem": "000001",  // Incremental: 000001, 000002, etc.
      "poNumber": "",  // Purchase Order number inside "purchase_order" key
      "poItem": "00010",  // Based on serial number: 1->00010, 2->00020, etc.
      "quantity": "",    // Quantity of that line item from tax invoice
      "unit": "",  // unit of that line item from tax invoice 
      "itemAmount": "",  // Total amount for that item before VAT from tax invoice
      "sheetNo": ""  // Will be auto-generated
    }}
  ]
}}

RULES:
1. Convert any date format to YYYYMMDD (e.g., "25/11/2025" -> "20251125")
2. companyCode is ALWAYS "2000"
3. currency is ALWAYS "SAR"
4. invoiceDocItem is incremental: first item is "000001", second is "000002", etc.
5. poItem is based on serial/line number: serial 1 = "00010", serial 2 = "00020", etc.
6. Extract grossAmount as the total invoice amount including VAT
7. Extract itemAmount as the total amount before VAT for each line item Tax Invoice(Not from PO items or GL Document Items). You MUST extract all items from every "items"/"line_items" array found inside "tax_invoice".
8. Look for invoice number in fields like: Invoice No, رقم الفاتورة, Invoice Number, Ref, etc.
9. Look for PO number in fields like: Purchase Order, PO, أمر الشراء, Order Number, etc. Its key-value pair will always be there in "purchase_order" key, pick the value from there.
10. quantity and unit should be fetched from tax invoice line item corresponding to itemAmount.
11. If posting date is not found, use document date
12. For sheetNo, I will generate random numbers - leave as empty string ""
13. Return ONLY valid JSON, no explanations


Return the transformed JSON object only."""

    try:
        completion = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            # model="openai/gpt-oss-120b",
            messages=[{
                "role": "user",
                "content": prompt
            }],
            temperature=0.1,
            response_format={"type": "json_object"},
        )
        
        result = json.loads(completion.choices[0].message.content)
        
        # Generate random sheetNo for each item
        if "item" in result and isinstance(result["item"], list):
            for item in result["item"]:
                if not item.get("sheetNo") or item.get("sheetNo") == "":
                    item["sheetNo"] = str(random.randint(1000000000, 9999999999))
                if item["poNumber"] == ("020007108" or "3020007108"):
                    item["poNumber"] = "3020006451"
                if item["poNumber"] == ("3030003277"):
                    item["poNumber"] = "3030003044"
                if item["poNumber"] == ("3030003358"):
                    item["poNumber"] = "3030003045"
        
        # Ensure fixed values are correct
        result["companyCode"] = "2000"
        result["currency"] = "SAR"
        
        return result
        
    except Exception as e:
        logger.error(f"Groq transformation failed: {e}")
        raise


def extract_all_pages(pdf_bytes):
    """
    Process PDF and extract OCR data using Qwen model, grouped by document type.
    Layer 1 extracts each page with document type identification,
    then merges pages of the same document type using code logic.
    
    Returns data grouped as: tax_invoice, purchase_order, gl_document
    """
    pages_images = pdf_to_images(pdf_bytes)
    
    # Initialize document type containers
    result = {
        "tax_invoice": None,
        "purchase_order": None,
        "gl_document": None
    }
    
    # Track pages for each document type (for merging multi-page docs)
    doc_pages = {
        "tax_invoice": [],
        "purchase_order": [],
        "gl_document": []
    }
    
    # Extract each page and categorize by document type
    for i, img_bytes in enumerate(pages_images):
        try:
            raw_json = extract_invoice_data(img_bytes)
            page_data = json.loads(raw_json)
            
            # Get document type from extraction
            doc_type = page_data.get("document_type", "").lower()
            data = page_data.get("data", page_data)
            
            # Map to our standard document types
            if doc_type in ["tax_invoice", "invoice"]:
                doc_pages["tax_invoice"].append(data)
            elif doc_type in ["purchase_order", "po"]:
                doc_pages["purchase_order"].append(data)
            elif doc_type in ["gl_document", "accounting_document"]:
                doc_pages["gl_document"].append(data)
            else:
                # Try to infer document type from data content
                data_str = json.dumps(data).lower()
                if any(kw in data_str for kw in ["فاتورة", "invoice", "vat", "ضريب"]):
                    doc_pages["tax_invoice"].append(data)
                elif any(kw in data_str for kw in ["purchase order", "po number", "أمر الشراء"]):
                    doc_pages["purchase_order"].append(data)
                elif any(kw in data_str for kw in ["gl", "posting", "debit", "credit", "مستند"]):
                    doc_pages["gl_document"].append(data)
                    
            logger.info(f"Page {i+1}: Identified as {doc_type or 'unknown'}")
            
        except Exception as e:
            logger.error(f"Failed to extract page {i+1}: {e}")
    
    # Merge pages for each document type
    for doc_type, pages in doc_pages.items():
        if len(pages) == 0:
            result[doc_type] = None
        elif len(pages) == 1:
            result[doc_type] = pages[0]
        else:
            # Merge multiple pages of the same document type
            result[doc_type] = merge_document_pages(pages)
    
    return result


def merge_document_pages(pages: list) -> dict:
    """
    Merge multiple pages of the same document type into one object.
    Combines line items arrays and preserves other fields.
    """
    if not pages:
        return None
    if len(pages) == 1:
        return pages[0]
    
    merged = {}
    items_keys = ["items", "line_items", "lineItems", "products", "entries", "بنود"]
    
    for page in pages:
        for key, value in page.items():
            # Check if this is a line items array that should be concatenated
            if key.lower() in [k.lower() for k in items_keys] and isinstance(value, list):
                if key not in merged:
                    merged[key] = []
                merged[key].extend(value)
            elif key not in merged:
                # First occurrence, just add it
                merged[key] = value
            elif merged[key] != value and value is not None:
                # If values differ and both non-null, prefer the more complete one
                if isinstance(value, (dict, list)) and not merged[key]:
                    merged[key] = value
                elif isinstance(value, str) and len(str(value)) > len(str(merged.get(key, ""))):
                    merged[key] = value
    
    return merged


def extract_and_transform(pdf_bytes):
    """
    Complete pipeline (2 Layers):
    - Layer 1: Extract all pages with Qwen model & merge by type
    - Layer 2: Transform to final structured JSON using Groq
    """
    # Layer 1: Extract and group by document type using Qwen
    extracted_data = extract_all_pages(pdf_bytes)
    
    # Layer 2: Transform extracted data to final format using Groq
    final_json = transform_to_final_json(extracted_data)
    
    return {
        "raw_extraction": extracted_data,
        "final_output": final_json
    }