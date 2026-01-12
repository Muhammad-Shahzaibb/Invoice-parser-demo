import os
import base64
import json
import fitz  # PyMuPDF
from groq import Groq
from dotenv import load_dotenv
import logging

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
if not GROQ_API_KEY:
    raise ValueError("GROQ_API_KEY not found in .env file!")

client = Groq(api_key=GROQ_API_KEY)
MAX_PAGES = int(os.getenv("MAX_PAGES", 5))  # configurable via env

# -------------------------
# Helpers
# -------------------------
def encode_image(image_bytes):
    return base64.b64encode(image_bytes).decode("utf-8")


def extract_invoice_data(base64_image):
    prompt = """
You are an expert invoice OCR and document understanding AI.
Extract all visible information from this image.
The document may contain Arabic, English, or both.

Rules:
- Capture ALL data visible on the page
- Do not invent or translate values
- Preserve original text exactly
- Group related information logically
- Numbers must remain numbers

Return ONE valid JSON object only.
No explanations, no markdown.
"""
    try:
        completion = client.chat.completions.create(
            model="meta-llama/llama-4-scout-17b-16e-instruct",
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
                ],
            }],
            temperature=0.1,
            response_format={"type": "json_object"},
        )
        return completion.choices[0].message.content
    except Exception as e:
        logger.error(f"Groq extraction failed: {e}")
        raise


def pdf_to_base64_images(pdf_bytes):
    """Convert PDF bytes to base64 images (up to MAX_PAGES)."""
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:
        logger.error(f"Failed to open PDF: {e}")
        raise

    page_count = min(len(doc), MAX_PAGES)
    pages_base64, pages_preview = [], []

    for page_index in range(page_count):
        try:
            page = doc.load_page(page_index)
            pix = page.get_pixmap(matrix=fitz.Matrix(1.5, 1.5))
            img_data = pix.tobytes("jpg")
            pages_base64.append(encode_image(img_data))
            pages_preview.append(img_data)
        except Exception as e:
            logger.error(f"Failed to process page {page_index+1}: {e}")

    doc.close()
    return pages_base64, pages_preview


def extract_all_pages(pdf_bytes):
    """Process PDF and extract OCR data from all pages."""
    pages_base64, _ = pdf_to_base64_images(pdf_bytes)

    final_output = {}
    for i, img_base64 in enumerate(pages_base64):
        try:
            raw_json = extract_invoice_data(img_base64)
            final_output[f"page_{i+1}"] = json.loads(raw_json)
        except Exception as e:
            logger.error(f"Failed to extract page {i+1}: {e}")
            final_output[f"page_{i+1}"] = {"error": str(e)}

    return final_output
