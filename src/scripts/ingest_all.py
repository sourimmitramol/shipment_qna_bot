# $env:PYTHONPATH='c:\Users\CHOWDHURYRaju\Desktop\shipment_qna_bot\src'; python src/scripts/reindex_data.py shipment_dec25.jsonl

# $env:PYTHONPATH='c:\Users\CHOWDHURYRaju\Desktop\shipment_qna_bot\src'; python src/scripts/ingest_all.py


import glob
import os
import shutil
import sys
import time
from datetime import timedelta

# Ensure src is in python path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from reindex_data import flatten_document, load_data

from shipment_qna_bot.tools.azure_ai_search import AzureAISearchTool
from shipment_qna_bot.tools.azure_openai_embeddings import \
    AzureOpenAIEmbeddingsClient


def robust_upload(tool, docs, batch_size=100, max_retries=3):
    """Uploads documents in batches with retry logic."""
    for i in range(0, len(docs), batch_size):
        batch = docs[i : i + batch_size]
        success = False
        retries = 0
        while not success and retries < max_retries:
            try:
                tool.upload_documents(batch)
                success = True
                print(
                    f"  Uploaded batch {i // batch_size + 1}/{(len(docs) - 1) // batch_size + 1}"
                )
            except Exception as e:
                retries += 1
                wait = retries * 5
                print(
                    f"  Batch upload failed (attempt {retries}): {e}. Retrying in {wait}s..."
                )
                time.sleep(wait)
        if not success:
            raise RuntimeError(
                f"Failed to upload batch starting at index {i} after {max_retries} attempts."
            )


def ingest_all():
    start_time = time.perf_counter()
    data_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "data")
    )
    processed_dir = os.path.join(data_dir, "processed")
    os.makedirs(processed_dir, exist_ok=True)

    jsonl_files = glob.glob(os.path.join(data_dir, "*.jsonl"))

    if not jsonl_files:
        print(f"No .jsonl files found in {data_dir}")
        return

    print(f"Found {len(jsonl_files)} files to ingest.")
    jsonl_files.sort()

    embedder = AzureOpenAIEmbeddingsClient()
    tool = AzureAISearchTool()

    for file_path in jsonl_files:
        file_name = os.path.basename(file_path)
        print(f"\n--- Starting ingestion for {file_name} ---")

        try:
            raw_docs = load_data(file_path)
        except Exception as e:
            print(f"Failed to read {file_name}: {e}. Skipping.")
            continue

        print(f"Loaded {len(raw_docs)} documents.")

        processed_docs = []
        print(f"Processing and embedding...")
        for i, d in enumerate(raw_docs):
            try:
                processed_docs.append(flatten_document(d, embedder))
                if (i + 1) % 100 == 0:
                    print(f"Processed {i+1}/{len(raw_docs)} docs...")
            except Exception as e:
                print(f"Failed to process doc {d.get('document_id')}: {e}")

        if processed_docs:
            print(f"Uploading {len(processed_docs)} docs in batches...")
            try:
                robust_upload(tool, processed_docs)
                print(f"Finished ingestion for {file_name}")

                # Move to processed folder
                dest_path = os.path.join(processed_dir, file_name)
                shutil.move(file_path, dest_path)
                print(f"Moved {file_name} to 'processed' folder.")
            except Exception as e:
                print(f"ERROR: Complete ingestion failed for {file_name}: {e}")
                print(f"File {file_name} remains in data folder for retry.")
        else:
            print(f"No documents to upload for {file_name}")

    end_time = time.perf_counter()
    delta = end_time - start_time

    print(f"Total time taken: {timedelta(seconds=round(delta))}")
    print("\n--- All ingestions complete! ---")


if __name__ == "__main__":
    ingest_all()
