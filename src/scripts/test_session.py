import asyncio
import os
import sys

# Ensure src is in python path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from langchain_core.messages import AIMessage, HumanMessage

from shipment_qna_bot.graph.builder import graph_app


async def run_turn(thread_id, question, consignee_codes):
    print(f"\n>>> USER: {question}")

    state = {
        "question_raw": question,
        "consignee_codes": consignee_codes,
        "conversation_id": thread_id,
        "retry_count": 0,
        "max_retries": 3,
        "is_satisfied": False,
        "messages": [HumanMessage(content=question)],
    }

    config = {"configurable": {"thread_id": thread_id}}

    try:
        # invoke handles state persistence via MemorySaver
        final_state = await graph_app.ainvoke(state, config=config)
        answer = final_state.get("answer_text")
        print(f"\n<<< BOT: {answer}")

        # Log if metrics are present
        usage = final_state.get("usage_metadata")
        if usage:
            print(f"Metrics: {usage}")

        # Log if loop happened
        retry_count = final_state.get("retry_count", 0)
        if retry_count > 0:
            print(f"(Reflection Loop triggered {retry_count} times)")

        return final_state
    except Exception as e:
        print(f"Error: {e}")
        return None


async def test_session():
    thread_id = "session-test-OOCU8049862"
    consignees = ["0025833"]

    # Turn 1
    await run_turn(thread_id, "Where is container OOCU8049862?", consignees)

    # Turn 2 - Follow up (contextual)
    await run_turn(thread_id, "When was it last updated?", consignees)

    # Turn 3 - Another follow up
    await run_turn(thread_id, "Is there any delay?", consignees)


if __name__ == "__main__":
    asyncio.run(test_session())
