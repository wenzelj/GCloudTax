import os
import json
import logging
from flask import Flask, request
from google.cloud import storage
from google.cloud import documentai_v1 as documentai # Recommended V1 API
from google.cloud.exceptions import NotFound, BadRequest

# --- Configuration & Logging ---
# Configure standard logging for Cloud Run/Cloud Functions integration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Use os.getenv for safer access with no default values (fail fast if not set)
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "valid-expanse-470905-f1") 
LOCATION = os.getenv("LOCATION", "us")
DOCUMENT_AI_PROCESSOR_ID = os.getenv("DOCUMENT_AI_PROCESSOR_ID", "4fc47710a3a194c8")

app = Flask(__name__)

# --- Initialization & Client Setup ---
# Initialize Google Cloud clients once globally
storage_client = None
documentai_client = None

def initialize_clients():
    """Initializes Google Cloud clients and validates configuration."""
    global storage_client, documentai_client
    
    if not all([PROJECT_ID, LOCATION, DOCUMENT_AI_PROCESSOR_ID]):
        logger.error("Critical configuration missing. Check PROJECT_ID, LOCATION, or PROCESSOR_ID environment variables.")
        raise EnvironmentError("Critical GCP configuration is missing.")

    try:
        logger.info(f"Initializing storage_client for project: {PROJECT_ID}")
        storage_client = storage.Client(project=PROJECT_ID)
        
        logger.info(f"Initializing documentai_client for location: {LOCATION}")
        # Construct the API endpoint for the client based on the location
        endpoint = f"{LOCATION}-documentai.googleapis.com"
        documentai_client = documentai.DocumentProcessorServiceClient(
            client_options={"api_endpoint": endpoint}
        )
        logger.info("All clients initialized successfully.")
    except Exception as e:
        logger.critical(f"Failed to initialize one or more GCP clients: {e}")
        # Re-raise to prevent the application from starting without essential services
        raise

# Initialize clients on startup
try:
    initialize_clients()
except EnvironmentError:
    # Exit if configuration is bad
    exit(1)
except Exception:
    # Exit if client initialization fails
    exit(1)


@app.route("/", methods=["POST"])
def process_document():
    # Ensure clients are ready before processing the request (safe guard)
    if storage_client is None or documentai_client is None:
        logger.error("GCP clients were not initialized properly.")
        return "Internal Server Error: Service not ready.", 503 # Service Unavailable
        
    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            logger.warning("Invalid JSON payload received.")
            return "Invalid JSON payload. Expected {'bucket_name': '...', 'file_name': '...'}", 400

        bucket_name = request_json.get("bucket_name")
        file_name = request_json.get("file_name")
        # generation is now handled implicitly by blob access for simplicity

        if not bucket_name or not file_name:
            logger.warning("Missing 'bucket_name' or 'file_name' in payload.")
            return "Missing 'bucket_name' or 'file_name' in payload", 400

        logger.info(f"Processing document: gs://{bucket_name}/{file_name}")

        # 1. Read PDF from Cloud Storage
        try:
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(file_name)
            pdf_content = blob.download_as_bytes()
            logger.info(f"Successfully downloaded {file_name} ({len(pdf_content)} bytes) from GCS.")
        except NotFound:
            logger.error(f"File not found: gs://{bucket_name}/{file_name}")
            return "File not found in Google Cloud Storage.", 404
        except Exception as e:
            logger.error(f"Failed to download PDF from GCS: {e}")
            return "Internal Server Error: Failed to download PDF.", 500


        # 2. Process PDF with Document AI
        try:
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
            logger.info("Received successful response from Document AI.")
        except BadRequest as e:
            # Handle API-specific client errors (e.g., bad format, wrong processor)
            logger.error(f"Document AI API Error (400 Bad Request): {e}")
            return f"Document AI processing failed (Bad Request): {e}", 400
        except Exception as e:
            logger.error(f"Failed to process document with Document AI: {e}")
            return "Internal Server Error: Document AI processing failed.", 500


        # 3. Comprehensive Entity Extraction
        extracted_entities = {}
        for entity in document.entities:
            # Extract all entities detected by the Document AI processor
            entity_type = entity.type_
            # Use confidence score for better data quality assessment (optional but good practice)
            entity_text = entity.mention_text
            
            # Group multiple entities of the same type into a list
            if entity_type not in extracted_entities:
                extracted_entities[entity_type] = []
            
            extracted_entities[entity_type].append(entity_text)
            
        logger.info(f"Extracted {len(document.entities)} entities from document.")
        
        # Log the extracted entities as structured JSON for easy filtering in Cloud Logging
        logger.info(json.dumps({"extracted_entities": extracted_entities}, indent=2))
        
        # (Optional) Log the full Document AI output only if debugging is necessary, as it can be very large.
        # document_json = json.dumps(document.to_json(), indent=2)
        # logger.debug(f"Full Document AI Output: {document_json}") 

        logger.info(f"Document gs://{bucket_name}/{file_name} processed successfully.")

        # Return the extracted entities as a successful response payload
        return json.dumps(extracted_entities), 200

    except Exception as e:
        logger.exception(f"Unhandled exception in process_document: {e}")
        return "Internal Server Error: An unexpected error occurred.", 500

if __name__ == "__main__":
    # Note: Cloud Run/Cloud Functions set the PORT env var automatically.
    port = int(os.environ.get("PORT", 8080))
    # It's better to run the app directly and let the execution environment handle the port
    app.run(host="0.0.0.0", port=port)