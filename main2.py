# # main.py (Updated Python code for the Summarization/Extraction Service)

# import os
# import json
# import base64 # Still needed if you decide to use Pub/Sub later
# from flask import Flask, request
# from google.cloud import storage
# from google.cloud import documentai_v1beta3 as documentai
# from google.cloud import secretmanager
# import psycopg2 # For PostgreSQL interaction

# app = Flask(__name__)

# # --- Configuration ---
# PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "valid-expanse-470905-f1")
# LOCATION = os.environ.get("LOCATION", "us-central1") # Region where your Cloud Run and Document AI processor are
# DOCUMENT_AI_PROCESSOR_ID = os.environ.get("DOCUMENT_AI_PROCESSOR_ID", "YOUR_DOCUMENT_AI_PROCESSOR_ID") # Replace with your actual processor ID
# DB_SECRET_NAME = os.environ.get("DB_SECRET_NAME", "tax-data-extractor-db-credentials") # Name of your Secret Manager secret

# # Initialize Google Cloud clients
# storage_client = storage.Client(project=PROJECT_ID)
# documentai_client = documentai.DocumentProcessorServiceClient(client_options={"api_endpoint": f"{LOCATION}-documentai.googleapis.com"}) # Specify endpoint for regional processor
# secret_client = secretmanager.SecretManagerServiceClient()

# # --- Database Connection (will be established on first request or at startup) ---
# db_connection = None

# def get_db_connection():
#     global db_connection
#     if db_connection is None:
#         try:
#             # Fetch database credentials from Secret Manager
#             secret_response = secret_client.access_secret_version(
#                 request={"name": f"projects/{PROJECT_ID}/secrets/{DB_SECRET_NAME}/versions/latest"}
#             )
#             db_credentials_json = secret_response.payload.data.decode("UTF-8")
#             db_credentials = json.loads(db_credentials_json)

#             # Cloud SQL Proxy or Private IP connection details
#             # For Cloud Run, typically you'd use a VPC Access Connector
#             # and connect via the private IP of your Cloud SQL instance.
#             # For simplicity, this example assumes direct connection (e.g., if public IP is enabled on Cloud SQL for testing)
#             # In production, strongly recommend Private IP + VPC Access Connector.
#             db_connection = psycopg2.connect(
#                 host=db_credentials["host"],
#                 database=db_credentials["database"],
#                 user=db_credentials["user"],
#                 password=db_credentials["password"]
#             )
#             print("Successfully connected to PostgreSQL database.")
#         except Exception as e:
#             print(f"Error connecting to database: {e}")
#             raise
#     return db_connection

# @app.route("/", methods=["POST"])
# def process_document():
#     try:
#         request_json = request.get_json(silent=True)
#         if not request_json:
#             return "Invalid JSON payload. Expected {'bucket_name': 'your-bucket', 'file_name': 'your-file.pdf'}", 400

#         bucket_name = request_json.get("bucket_name")
#         file_name = request_json.get("file_name")
#         generation = request_json.get("generation") # Optional, for specific file versions

#         if not bucket_name or not file_name:
#             return "Missing 'bucket_name' or 'file_name' in payload", 400

#         print(f"Processing document: gs://{bucket_name}/{file_name}")

#         # 1. Read PDF from Cloud Storage
#         bucket = storage_client.bucket(bucket_name)
#         blob = bucket.blob(file_name, generation=generation)
#         pdf_content = blob.download_as_bytes()

#         # 2. Process PDF with Document AI
#         processor_path = documentai_client.processor_path(PROJECT_ID, LOCATION, DOCUMENT_AI_PROCESSOR_ID)
#         raw_document = documentai.RawDocument(
#             content=pdf_content,
#             mime_type="application/pdf"
#         )
#         request_doc_ai = documentai.ProcessRequest(
#             name=processor_path,
#             raw_document=raw_document
#         )

#         response_doc_ai = documentai_client.process_document(request=request_doc_ai)
#         document = response_doc_ai.document

#         # 3. Extract relevant data from Document AI response
#         # This part is highly dependent on your document structure and processor output
#         extracted_data = {"document_path": f"gs://{bucket_name}/{file_name}"}
#         tax_id_found = False
#         tax_amount_found = False

#         for entity in document.entities:
#             # Example: Extracting fields from a Form Parser output
#             # You'll need to inspect the Document AI output for your specific document type
#             # and adjust these entity.type_ values accordingly.
#             if entity.type_ == "tax_id" and not tax_id_found: # Assuming 'tax_id' is a field
#                 extracted_data["tax_id"] = entity.mention_text
#                 tax_id_found = True
#             elif entity.type_ == "total_tax_amount" and not tax_amount_found: # Assuming 'total_tax_amount' is a field
#                 extracted_data["tax_amount"] = entity.mention_text
#                 tax_amount_found = True
#             # Add more fields as needed based on your Document AI processor's output

#         # If specific fields aren't found, you might want to log it or set to None
#         if not tax_id_found:
#             extracted_data["tax_id"] = None
#         if not tax_amount_found:
#             extracted_data["tax_amount"] = None

#         print(f"Extracted data: {extracted_data}")

#         # 4. Store extracted data in PostgreSQL
#         conn = get_db_connection()
#         cursor = conn.cursor()
#         # Ensure your table 'extracted_tax_data' exists with appropriate columns
#         insert_query = """
#         INSERT INTO extracted_tax_data (document_path, tax_id, tax_amount, raw_doc_ai_response)
#         VALUES (%s, %s, %s, %s::jsonb);
#         """
#         cursor.execute(insert_query, (
#             extracted_data["document_path"],
#             extracted_data["tax_id"],
#             extracted_data["tax_amount"],
#             document.to_json() # Store full Document AI response as JSONB
#         ))
#         conn.commit()
#         cursor.close()

#         # 5. Archive processed document (optional, but good practice)
#         # For now, let's just print a message. You can implement moving/copying here.
#         print(f"Document gs://{bucket_name}/{file_name} processed and archived (conceptually).")

#         return "Document processed successfully", 200

#     except Exception as e:
#         print(f"Error processing document: {e}")
#         # In a real application, you'd want more sophisticated error handling and logging
#         return f"Internal Server Error: {e}", 500

# if __name__ == "__main__":
#     app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
