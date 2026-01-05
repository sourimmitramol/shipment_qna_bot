# $env:PYTHONPATH='c:\Users\CHOWDHURYRaju\Desktop\shipment_qna_bot\src'; python src/scripts/reindex_data.py shipment_dec25.jsonl

import json
import os
import sys
from typing import Any, Dict, List

# Ensure src is in python path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from shipment_qna_bot.tools.azure_ai_search import AzureAISearchTool
from shipment_qna_bot.tools.azure_openai_embeddings import \
    AzureOpenAIEmbeddingsClient


def load_data(file_path: str) -> List[Dict[str, Any]]:
    documents = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                documents.append(record)
            except json.JSONDecodeError as e:
                print(f"Skipping invalid line: {e}")
    return documents


def flatten_document(
    doc: Dict[str, Any], embedder: AzureOpenAIEmbeddingsClient
) -> Dict[str, Any]:
    metadata = doc.get("metadata", {})
    content = doc.get("content", "")
    doc_id = doc.get("document_id")

    # Consignee codes (RLS)
    consignee_codes = metadata.get("consignee_codes", [])
    if not consignee_codes:
        raw = doc.get("consignee_code")
        if raw:
            try:
                consignee_codes = json.loads(raw.replace("'", '"'))
            except:
                consignee_codes = [raw]

    # Geenerate embedding
    print(f"Generating embedding for doc {doc_id}...")
    vector = embedder.embed_query(content)

    def to_list(val):
        if val is None:
            return []
        if isinstance(val, list):
            # Flatten any nested lists and ensure all elements are strings
            flat = []
            for item in val:
                if isinstance(item, str) and "," in item:
                    flat.extend([s.strip() for s in item.split(",") if s.strip()])
                else:
                    flat.append(str(item))
            return list(set(flat))
        if isinstance(val, str):
            if "," in val:
                return [s.strip() for s in val.split(",") if s.strip()]
            return [val.strip()]
        return [str(val)]

    flattened = {
        "document_id": str(doc_id),
        "content": content,
        "content_vector": vector,
        "consignee_code_ids": to_list(consignee_codes),
        "container_number": metadata.get("container_number"),
        "po_numbers": to_list(metadata.get("po_numbers", [])),
        "obl_nos": to_list(metadata.get("obl_nos", [])),
        "booking_numbers": to_list(metadata.get("booking_numbers", [])),
        "shipment_status": metadata.get("shipment_status"),
        "eta_dp_date": metadata.get("eta_dp_date"),
        "optimal_ata_dp_date": metadata.get("optimal_ata_dp_date"),
        "optimal_eta_fd_date": metadata.get("optimal_eta_fd_date"),
        # Metadata JSON blob
        "metadata_json": json.dumps(metadata),
    }

    # Handle dates if present
    # ... (simplifying for now, can add more later if needed)

    return flattened


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Ingest JSONL data into Azure Search index."
    )
    parser.add_argument(
        "file_name",
        nargs="?",
        default="shipment_dec25.jsonl",
        help="Name of the file in data/ directory",
    )
    args = parser.parse_args()

    data_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "data", args.file_name
    )
    if not os.path.exists(data_path):
        print(f"Data file not found: {data_path}")
        return

    raw_docs = load_data(data_path)
    print(f"Loaded {len(raw_docs)} documents from {args.file_name}.")

    embedder = AzureOpenAIEmbeddingsClient()

    processed_docs = []
    print(
        f"Starting processing and embedding (this may take a few minutes for {len(raw_docs)} docs)..."
    )

    for i, d in enumerate(raw_docs):
        try:
            processed_docs.append(flatten_document(d, embedder))
            if (i + 1) % 100 == 0:
                print(f"Processed {i+1}/{len(raw_docs)} docs...")
        except Exception as e:
            print(f"Failed to process doc {d.get('document_id')}: {e}")

    print(f"Uploading {len(processed_docs)} docs to the index...")
    tool = AzureAISearchTool()
    try:
        # Use batching for upload if possible, tool.upload_documents usually handles this
        tool.upload_documents(processed_docs)
        print("Full re-indexing complete!")
    except Exception as e:
        print(f"Upload failed: {e}")


if __name__ == "__main__":
    main()
