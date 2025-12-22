import asyncio
import os
import sys

# Ensure src is in python path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from langchain_core.messages import HumanMessage

from shipment_qna_bot.graph.builder import graph_app


async def test_identifier_retrieval(identifier_type, identifier_value):
    print(f"\n>>> TESTING {identifier_type}: {identifier_value}")

    state = {
        "question_raw": f"Where is {identifier_type} {identifier_value}?",
        "consignee_codes": ["0025833"],
        "retry_count": 0,
        "max_retries": 3,
        "is_satisfied": False,
        "usage_metadata": {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        },
        "messages": [
            HumanMessage(content=f"Where is {identifier_type} {identifier_value}?")
        ],
    }

    config = {
        "configurable": {"thread_id": f"test-{identifier_type}-{identifier_value}"}
    }

    try:
        final_state = await graph_app.ainvoke(state, config=config)
        answer = final_state.get("answer_text")
        plan = final_state.get("retrieval_plan")
        hits = final_state.get("hits", [])

        print(f"PLAN: {plan}")
        print(f"HITS COUNT: {len(hits)}")
        print(f"<<< BOT: {answer[:200]}...")
    except Exception as e:
        print(f"Error: {e}")


async def main():
    # Test cases based on user feedback
    # PO number (usually 10 digits in this app's context)
    await test_identifier_retrieval("PO", "5302997239")

    # OBL number
    await test_identifier_retrieval("OBL", "OOCU1234567890")  # Dummy but likely format


if __name__ == "__main__":
    asyncio.run(main())
