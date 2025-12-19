from typing import Any, Dict

from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context
from shipment_qna_bot.tools.azure_openai_chat import AzureOpenAIChatTool

_CHAT_TOOL = None


def _get_chat_tool() -> AzureOpenAIChatTool:
    global _CHAT_TOOL
    if _CHAT_TOOL is None:
        _CHAT_TOOL = AzureOpenAIChatTool()
    return _CHAT_TOOL


def answer_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Synthesizes a natural language answer from retrieved documents using LLM.
    """
    set_log_context(
        conversation_id=state.get("conversation_id", "-"),
        consignee_codes=state.get("consignee_codes", []),
        intent=state.get("intent", "-"),
    )

    with log_node_execution(
        "Answer",
        {
            "intent": state.get("intent", "-"),
            "hits_count": len(state.get("hits") or []),
        },
    ):
        hits = state.get("hits") or []
        analytics = state.get("idx_analytics") or {}
        question = state.get("question_raw") or ""

        # Context construction
        context_str = ""

        # 1. Add Analytics Context
        if analytics:
            count = analytics.get("count")
            facets = analytics.get("facets")
            context_str += f"--- Analytics Data ---\nTotal Matches: {count}\n"
            if facets:
                context_str += f"Facets: {facets}\n"

        # 2. Add Documents Context
        if hits:
            for i, hit in enumerate(hits[:5]):
                context_str += f"\n--- Document {i+1} ---\n"
                # Sort keys to keep output predictable
                for key in sorted(hit.keys()):
                    val = hit[key]
                    if val is not None and key not in [
                        "score",
                        "reranker_score",
                        "doc_id",
                    ]:
                        context_str += f"{key}: {val}\n"

        # If no info at all
        if not hits and not analytics:
            state["answer_text"] = (
                "I couldn't find any information matching your request within your authorized scope."
            )
            return state

        # Prompt Construction
        system_prompt = """
Role:
You are an expert in Data Analysis AI/ML, specializing in using the pandas library for accurate, efficient, and insightful data exploration.

Goal:
Your primary function is to analyze retrieved shipment and logistics data to answer user questions, summarize findings, and extract key information without fabricating any data.

Context & Constraints:
- Source of Truth: Use ONLY the data retrieved from Azure AI Search (shipment index). Do not use external web knowledge.
- Data Integrity: Never invent or hallucinate data, columns, or records. If the required data is not present, say so explicitly.
- Always give date when giving response in dd-mmm-yy format
Result Limitation & Pagination:
- When a query would result in more than 20 rows of raw record output, do NOT print them all.
- Instead, summarize: e.g. "Found 145 shipments; average delay is 7 days", and include a placeholder tag [ACTION: SHOW_MORE] in your answer.

Output Format:
a. Direct Answer
b. Summary & Methodology
c. Data Preview (if applicable; 5–10 rows max)
d. Pagination Signal [ACTION: SHOW_MORE] (if applicable)
""".strip()

        user_prompt = (
            f"Context:\n{context_str}\n\n" f"Question: {question}\n\n" "Answer:"
        )

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        try:
            chat_tool = _get_chat_tool()
            response_text = chat_tool.chat_completion(messages)
            state["answer_text"] = response_text

            logger.info(
                f"Generated answer: {response_text[:100]}...",
                extra={"step": "NODE:Answer"},
            )
        except Exception as e:
            logger.error(f"LLM generation failed: {e}", exc_info=True)
            state["answer_text"] = (
                "I found relevant documents but encountered an error generating the summary. "
                "Please check the evidence logs."
            )
            state.setdefault("errors", []).append(f"LLM Error: {e}")

        return state
