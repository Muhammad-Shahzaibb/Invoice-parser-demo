"""
Test Script for SAP Retention (F-43) API
Correct handling of $format and POST requests
"""

import json
import asyncio
import httpx
import socket
import warnings

# -------------------------
# SAP Configuration
# -------------------------
SAP_BASE_URL = "https://asd.al-akaria.com/sap/opu/odata/sap/ZFI_F_43_API_SRV"
SAP_ENTITY = "FHeaderSet"
SAP_CLIENT = "110"

SAP_GET_URL = f"{SAP_BASE_URL}/{SAP_ENTITY}?sap-client={SAP_CLIENT}&$format=json"
SAP_POST_URL = f"{SAP_BASE_URL}/{SAP_ENTITY}?sap-client={SAP_CLIENT}"

SAP_RETENTION_AUTH = ("OHUSSAIN", "Skr@@244343P")


async def test_retention_api():
    """
    Test SAP Retention API with the saved payload.
    """

    # -------------------------
    # Load Payload
    # -------------------------
    with open("test_retention_payload.json", "r") as f:
        payload = json.load(f)

    print("=" * 70)
    print("SAP RETENTION (F-43) API TEST")
    print("=" * 70)
    print(f"\nPOST Endpoint: {SAP_POST_URL}")
    print(f"Auth User: {SAP_RETENTION_AUTH[0]}")
    print("\nPayload:")
    print(json.dumps(payload, indent=2))
    print("\n" + "=" * 70)
    # -------------------------
    # SAP API Call
    # -------------------------
    async with httpx.AsyncClient(
        auth=SAP_RETENTION_AUTH,
        verify=False,
        timeout=30.0,
    ) as client:

        try:
            # -------------------------
            # Step 1: Fetch CSRF Token
            # -------------------------
            print("\nStep 1: Fetching CSRF Token...")

            fetch_res = await client.get(
                SAP_GET_URL,
                headers={
                    "x-csrf-token": "fetch",
                    "Accept": "application/json",
                },
            )

            print(f"GET Status Code: {fetch_res.status_code}")

            csrf_token = fetch_res.headers.get("x-csrf-token")

            if not csrf_token:
                print("❌ Failed to fetch CSRF token")
                print(fetch_res.text)
                return {"error": "Failed to fetch CSRF token"}

            print("✅ CSRF Token received")

            # -------------------------
            # Step 2: POST Data
            # -------------------------
            print("\nStep 2: Posting data to SAP...")

            headers = {
                "x-csrf-token": csrf_token,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }

            sap_res = await client.post(
                SAP_POST_URL,
                headers=headers,
                json=payload,
            )

            print(f"POST Status Code: {sap_res.status_code}")
            print("Response Content-Type:", sap_res.headers.get("Content-Type"))

            print("\nResponse Body:\n")

            # -------------------------
            # Safe Response Handling
            # -------------------------
            content_type = sap_res.headers.get("Content-Type", "")

            if "application/json" in content_type:
                response_json = sap_res.json()
                print(json.dumps(response_json, indent=2))

                # Success detection
                if sap_res.status_code in [200, 201]:
                    print("\n" + "=" * 70)
                    print("✅ SUCCESS: Document posted to SAP successfully!")
                    print("=" * 70)

                    # Extract document number if present
                    try:
                        doc_no = response_json.get("d", {}).get("DOC_NO")
                        if doc_no:
                            print(f"\n📄 Created Document Number: {doc_no}")
                    except:
                        pass

                return response_json

            else:
                print("⚠ SAP returned non-JSON response")
                print(sap_res.text)

                return {
                    "status_code": sap_res.status_code,
                    "raw_response": sap_res.text,
                }

        except Exception as e:
            import traceback

            print("\n" + "=" * 70)
            print("❌ SAP Retention API Connection Failed")
            print(f"Exception: {str(e)}")
            print(traceback.format_exc())
            print("=" * 70)

            return {
                "error": str(e),
                "error_type": type(e).__name__,
                "traceback": traceback.format_exc(),
            }


if __name__ == "__main__":
    print("\n🔧 Starting SAP Retention API Test...\n")

    warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    result = asyncio.run(test_retention_api())

    print("\n\n📊 Final Result:")
    print(json.dumps(result, indent=2))
