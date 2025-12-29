import json
from datetime import datetime

from shipment_qna_bot.graph.builder import run_graph


def test_date_grounding():
    print("Testing date grounding and tool usage...")
    today_actual = datetime.now().strftime("%Y-%m-%d")
    print(f"Actual today's date: {today_actual}")

    questions = ["What is the date today?", "Show me shipments arriving today"]

    results = []
    for q in questions:
        print(f"\n--- Testing Question: {q} ---")
        state = {
            "conversation_id": "final_verify_date_tool",
            "question_raw": q,
            "consignee_codes": ["0025833"],
            "messages": [],
        }

        try:
            result = run_graph(state)
            answer = result.get("answer_text")
            plan = result.get("retrieval_plan")
            print(f"Plan: {plan}")
            print(f"Answer: {answer}")
            results.append({"question": q, "plan": plan, "answer": answer})
        except Exception as e:
            print(f"Error: {e}")
            results.append({"question": q, "error": str(e)})

    with open("final_verify_output.txt", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print("\nVerification complete. Results saved to final_verify_output.txt")


if __name__ == "__main__":
    test_date_grounding()
