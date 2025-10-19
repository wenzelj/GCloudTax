# main.py (Updated Python code for the Summarization/Extraction Service - Logging only)

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
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "valid-expanse-470905-f1")
LOCATION = os.environ.get("LOCATION", "us-central1") # Region where your Cloud Run and Document AI processor are
DOCUMENT_AI_PROCESSOR_ID = os.environ.get("DOCUMENT_AI_PROCESSOR_ID", "4fc47710a3a194c8") # Replace with your actual processor ID

# Initialize Google Cloud clients
storage_client = storage.Client(project=PROJECT_ID)
documentai_client = documentai.DocumentProcessorServiceClient(client_options={"api_endpoint": f"{LOCATION}-documentai.googleapis.com"})

@app.route("/", methods=["POST"])
def process_document():
    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            return "Invalid JSON payload. Expected {'bucket_name': 'your-bucket', 'file_name': 'your-file.pdf'}", 400

        bucket_name = request_json.get("bucket_name")
        file_name = request_json.get("file_name")
        generation = request_json.get("generation") # Optional, for specific file versions

        if not bucket_name or not file_name:
            return "Missing 'bucket_name' or 'file_name' in payload", 400

        print(f"Processing document: gs://{bucket_name}/{file_name}")

        # 1. Read PDF from Cloud Storage
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name, generation=generation)
        pdf_content = blob.download_as_bytes()

        # 2. Process PDF with Document AI
        processor_path = documentai_client.processor_path(PROJECT_ID, LOCATION, DOCUMENT_AI_PROCESSOR_ID)
        raw_document = documentai.RawDocument(
            content=pdf_content,
            mime_type="application/pdf"
        )
        request_doc_ai = documentai.ProcessRequest(
            name=processor_path,
            raw_document=raw_document
        )

        response_doc_ai = documentai_client.process_document(request=request_doc_ai)
        document = response_doc_ai.document

        # --- IMPORTANT CHANGE: Log the full Document AI output ---
        # Convert the Document object to a JSON string for easy viewing in logs
        document_json = json.dumps(document.to_json(), indent=2)
        print(f"--- Full Document AI Output for {file_name} ---")
        print(document_json)
        print(f"--- End Document AI Output for {file_name} ---")

        # 3. (Optional) Basic extraction for quick overview
        extracted_summary = {}
        for entity in document.entities:
            # You can still do some basic parsing here to get a quick summary
            # but the full JSON is what we want to inspect.
            if entity.type_ == "tax_id":
                extracted_summary["tax_id"] = entity.mention_text
            elif entity.type_ == "total_tax_amount":
                extracted_summary["total_tax_amount"] = entity.mention_text
            # Add other key fields you expect

        print(f"Quick extracted summary: {extracted_summary}")

        # Removed: Database interaction

        # 4. Archive processed document (optional, but good practice)
        # For now, let's just print a message. You can implement moving/copying here.
        print(f"Document gs://{bucket_name}/{file_name} processed (Document AI output logged).")

        return "Document processed and output logged successfully", 200

    except Exception as e:
        print(f"Error processing document: {e}")
        return f"Internal Server Error: {e}", 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
