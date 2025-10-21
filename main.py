# main.py (Updated Python code for the Summarization/Extraction Service - with more debugging)

import os
import json
import base64
from flask import Flask, request
from google.cloud import storage
from google.cloud import documentai_v1beta3 as documentai
# Removed: from google.cloud import secretmanager
# Removed: import psycopg2

app = Flask(__name__)

# --- Configuration ---
PROJECT_ID = os.environ.get("GCP_PROJECT_ID") # Get directly from env var
LOCATION = os.environ.get("LOCATION")       # Get directly from env var
DOCUMENT_AI_PROCESSOR_ID = "4fc47710a3a194c8"

# Add more print statements for debugging environment variables
print(f"DEBUG: PROJECT_ID from env: {PROJECT_ID}")
print(f"DEBUG: LOCATION from env: {LOCATION}")
print(f"DEBUG: DOCUMENT_AI_PROCESSOR_ID from env: {DOCUMENT_AI_PROCESSOR_ID}")

# Validate essential environment variables early
if not PROJECT_ID:
    print("ERROR: GCP_PROJECT_ID environment variable is not set!")
    exit(1) # Exit early if critical config is missing
if not LOCATION:
    print("ERROR: LOCATION environment variable is not set!")
    exit(1)
if not DOCUMENT_AI_PROCESSOR_ID:
    print("ERROR: DOCUMENT_AI_PROCESSOR_ID environment variable is not set!")
    exit(1)


# Initialize Google Cloud clients with explicit error handling
storage_client = None
documentai_client = None

try:
    print("DEBUG: Initializing storage_client...")
    storage_client = storage.Client(project=PROJECT_ID)
    print("DEBUG: storage_client initialized.")
except Exception as e:
    print(f"ERROR: Failed to initialize storage_client: {e}")
    exit(1) # Exit if client fails to initialize

try:
    print("DEBUG: Initializing documentai_client...")
    # Ensure the API endpoint is correctly formed and used
    documentai_client = documentai.DocumentProcessorServiceClient(client_options={"api_endpoint": f"{LOCATION}-documentai.googleapis.com"})
    print("DEBUG: documentai_client initialized.")
except Exception as e:
    print(f"ERROR: Failed to initialize documentai_client: {e}")
    exit(1) # Exit if client fails to initialize


@app.route("/", methods=["POST"])
def process_document():
    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            print("ERROR: Invalid JSON payload received.")
            return "Invalid JSON payload. Expected {'bucket_name': 'your-bucket', 'file_name': 'your-file.pdf'}", 400

        bucket_name = request_json.get("bucket_name")
        file_name = request_json.get("file_name")
        generation = request_json.get("generation") # Optional, for specific file versions

        if not bucket_name or not file_name:
            print("ERROR: Missing 'bucket_name' or 'file_name' in payload.")
            return "Missing 'bucket_name' or 'file_name' in payload", 400

        print(f"Processing document: gs://{bucket_name}/{file_name}")

        # 1. Read PDF from Cloud Storage
        try:
            print(f"DEBUG: Attempting to get blob for gs://{bucket_name}/{file_name}")
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(file_name, generation=generation)
            pdf_content = blob.download_as_bytes()
            print(f"DEBUG: Successfully downloaded {file_name} from GCS.")
        except Exception as e:
            print(f"ERROR: Failed to download PDF from GCS: {e}")
            return f"Internal Server Error: Failed to download PDF: {e}", 500


        # 2. Process PDF with Document AI
        try:
            processor_path = documentai_client.processor_path(PROJECT_ID, LOCATION, DOCUMENT_AI_PROCESSOR_ID)
            print(f"DEBUG: Document AI Processor Path: {processor_path}")
            raw_document = documentai.RawDocument(
                content=pdf_content,
                mime_type="application/pdf"
            )
            request_doc_ai = documentai.ProcessRequest(
                name=processor_path,
                raw_document=raw_document
            )
            print("DEBUG: Sending request to Document AI...")
            response_doc_ai = documentai_client.process_document(request=request_doc_ai)
            document = response_doc_ai.document
            print("DEBUG: Received response from Document AI.")
        except Exception as e:
            print(f"ERROR: Failed to process document with Document AI: {e}")
            return f"Internal Server Error: Document AI processing failed: {e}", 500


        # --- IMPORTANT CHANGE: Log the full Document AI output ---
        # Convert the Document object to a JSON string for easy viewing in logs
        document_json = json.dumps(document.to_json(), indent=2)
        print(f"--- Full Document AI Output for {file_name} ---")
        print(document_json)
        print(f"--- End Document AI Output for {file_name} ---")

        # 3. (Optional) Basic extraction for quick overview
        extracted_summary = {}
        for entity in document.entities:
            if entity.type_ == "tax_id":
                extracted_summary["tax_id"] = entity.mention_text
            elif entity.type_ == "total_tax_amount":
                extracted_summary["total_tax_amount"] = entity.mention_text

        print(f"Quick extracted summary: {extracted_summary}")

        print(f"Document gs://{bucket_name}/{file_name} processed (Document AI output logged).")

        return "Document processed and output logged successfully", 200

    except Exception as e:
        print(f"ERROR: Unhandled exception in process_document: {e}")
        return f"Internal Server Error: {e}", 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
