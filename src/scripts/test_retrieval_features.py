import asyncio
import os
import sys

from dotenv import load_dotenv

# Ensure src is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

load_dotenv()

from shipment_qna_bot.graph.nodes.extractor import extractor_node
from shipment_qna_bot.graph.nodes.intent import intent_node
from shipment_qna_bot.graph.nodes.planner import planner_node
from shipment_qna_bot.tools.azure_ai_search import AzureAISearchTool


async def test_retrieval_features():
    print("--- Testing AzureAISearchTool with Pagination & Sorting ---")
    tool = AzureAISearchTool()
    print(f"DEBUG: Index Name: {tool._client._index_name}")
    print(f"DEBUG: Consignee Field: {tool._consignee_field}")
    print(f"DEBUG: Is Collection: {tool._consignee_is_collection}")
    print(f"DEBUG: Endpoint: {tool._client._endpoint}")
    print(f"DEBUG: Doc Count: {tool._client.get_document_count()}")

    valid_code = "0025833"
    print("\n[Test Raw] Search * without filter...")
    try:
        raw_res = list(tool._client.search(search_text="*", top=1))
        count = len(raw_res)
        if count > 0:
            r = raw_res[0]
            codes = r.get("consignee_code_ids")
            if codes and len(codes) > 0:
                valid_code = codes[0]
            print(f"Raw Hit: {r.get('container_number')} | Consignee: {codes}")
        print(f"Raw Hits Count: {count}")
        print(f"Usage Code for Test: {valid_code}")
    except Exception as e:
        print(f"Raw Search Failed: {e}")

    # Test 0: Basic Search (Sanity Check)
    print(f"\n[Test 0] Basic Search (No Sort/Skip) for {valid_code}...")
    try:
        res0 = tool.search(query_text="*", consignee_codes=[valid_code], top_k=2)
        print(f"Hits: {len(res0['hits'])}")
        for h in res0["hits"]:
            print(
                f" - Container: {h.get('container_number')} | ID: {h.get('document_id')}"
            )
    except Exception as e:
        print(f"Test 0 Failed: {e}")

    # Test 1: Sorting (Current data)
    print(f"\n[Test 1] Sorting by optimal_eta_fd_date desc for {valid_code}...")
    try:
        res = tool.search(
            query_text="*",
            consignee_codes=[valid_code],
            top_k=2,
            order_by="optimal_eta_fd_date desc",
        )
        print(f"Hits: {len(res['hits'])}")
        for h in res["hits"]:
            print(
                f" - Container: {h.get('container_number')} | ETA FD: {h.get('optimal_eta_fd_date')}"
            )
    except Exception as e:
        print(f"Test 1 Failed: {e}")

    # Test 2: Pagination (Skip 2)
    print(f"\n[Test 2] Pagination (Skip 2) for {valid_code}...")
    try:
        res_skip = tool.search(
            query_text="*",
            consignee_codes=[valid_code],
            top_k=2,
            skip=2,
            order_by="optimal_eta_fd_date desc",
        )
        print(f"Hits: {len(res_skip['hits'])}")
        for h in res_skip["hits"]:
            print(
                f" - Container: {h.get('container_number')} | ETA FD: {h.get('optimal_eta_fd_date')}"
            )
    except Exception as e:
        print(f"Test 2 Failed: {e}")

    # Test 3: Intent & Sentiment
    print("\n--- Testing Intent Node (Sentiment) ---")
    mock_state = {
        "normalized_question": "I am very angry about the delay of container SEGU1234567"
    }
    try:
        res_intent = intent_node(mock_state)
        print(f"Input: {mock_state['normalized_question']}")
        print(f"Result: {res_intent}")
    except Exception as e:
        print(f"Test 3 Failed: {e}")

    # Test 4: Multiple Consignees
    print("\n--- Testing Multiple Consignee Codes ---")
    try:
        # Assuming RLS handles list correctly
        # We might not have data for a second code, but we check if it errors
        res_multi = tool.search(
            query_text="*", consignee_codes=["0025833", "9999999"], top_k=1
        )
        print(f"Search with multiple codes success. Hits: {len(res_multi['hits'])}")
    except Exception as e:
        print(f"Test 4 Failed: {e}")

    # Test 5: Planner for "Next 10"
    print("\n--- Testing Planner for 'Next 10' ---")
    mock_state_planner = {
        "normalized_question": "show me the next 10 results",
        "extracted_ids": {},
        "conversation_id": "test",
        "consignee_codes": ["0025833"],
    }
    try:
        res_planner = planner_node(mock_state_planner)
        plan = res_planner.get("retrieval_plan")
        print(f"Plan: {plan}")
    except Exception as e:
        print(f"Test 5 Failed: {e}")


if __name__ == "__main__":
    asyncio.run(test_retrieval_features())
