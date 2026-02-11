import os
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials

CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE = "token.json"
SCOPES = ['https://www.googleapis.com/auth/gmail.send']


def generate_token():
    if not os.path.exists(CREDENTIALS_FILE):
        print(f"ERROR: {CREDENTIALS_FILE} not found.")
        print("Download it from: https://console.cloud.google.com/")
        print("APIs & Services → Credentials → OAuth 2.0 Client IDs → Download JSON")
        return

    print("Opening browser for Google OAuth login...")
    flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
    creds = flow.run_local_server(port=0)

    # Save credentials as JSON (not pickle)
    with open(TOKEN_FILE, 'w') as token:
        token.write(creds.to_json())

    print(f"\n✅ token.json generated successfully!")
    print(f"   Deploy this file to your server alongside credentials.json.")
    print(f"   The app will auto-refresh the token when it expires.")


if __name__ == "__main__":
    generate_token()
