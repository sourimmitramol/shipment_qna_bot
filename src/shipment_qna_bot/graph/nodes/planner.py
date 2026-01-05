import json
import re
from typing import Any, Dict, List, Optional

from shipment_qna_bot.graph.state import RetrievalPlan
from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context
from shipment_qna_bot.tools.azure_openai_chat import AzureOpenAIChatTool

_CHAT_TOOL = None


def _get_chat_tool() -> AzureOpenAIChatTool:
    global _CHAT_TOOL
    if _CHAT_TOOL is None:
        _CHAT_TOOL = AzureOpenAIChatTool()
    return _CHAT_TOOL


def planner_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generates a RetrievalPlan (query text, top_k, filters) using LLM and extracted metadata.
    """
    set_log_context(
        conversation_id=state.get("conversation_id", "-"),
        consignee_codes=state.get("consignee_codes", []),
        intent=state.get("intent", "-"),
    )

    with log_node_execution(
        "Planner",
        {
            "intent": state.get("intent", "-"),
            "question": (state.get("normalized_question") or "-")[:120],
        },
    ):
        q = (
            state.get("normalized_question") or state.get("question_raw") or ""
        ).strip()
        extracted = state.get("extracted_ids") or {}

        # Logistics mapping and synonym dictionary for the LLM
        logistics_context = """
        Field Mappings in Index:
        - container_number (String): e.g. SEGU5935510. Use: container_number eq '...' or contains(container_number, '...')
        - po_numbers (Collection): e.g. 5302997239. Use: po_numbers/any(p: p eq '...')
        - booking_numbers (Collection): e.g. TH2017996. Use: booking_numbers/any(b: b eq '...')
        - obl_nos (Collection): e.g. OBL123. Use: obl_nos/any(o: o eq '...')
        - shipment_status (String): DELIVERED, IN_OCEAN, AT_DISCHARGE_PORT, READY_FOR_PICKUP, EMPTY_RETURNED. Use: shipment_status eq '...'
        - hot_container_flag (Boolean): true/false. Use: hot_container_flag eq true
        - discharge_port_name (String): e.g. "Los Angeles". Use: contains(discharge_port_name, '...')
        - mother_vessel_name (String): e.g. "MAERSK SERANGOON". Use: contains(mother_vessel_name, '...')

        Synonyms & OData Tips:
        - "on water", "sailing" -> shipment_status eq 'IN_OCEAN'
        - "hot" -> hot_container_flag eq true
        - For ID Collections (PO, Booking, OBL), ALWAYS use 'any(p: p eq '...')' syntax.
        - For descriptive fields (Port, Vessel), 'contains(field, '...')' is more flexible than 'eq'.
    """.strip()

        system_prompt = f"""
        You are a Search Planner for a logistics bot. Given a user question and extracted entities, generate an Azure Search Plan.
        
        {logistics_context}

        Output JSON only:
        {{
            "query_text": "text for hybrid search",
            "top_k": number (default 20, max 100),
            "extra_filter": "OData filter string or null",
            "reason": "short explanation"
        }}
        """

        reflection_feedback = state.get("reflection_feedback")
        retry_count = state.get("retry_count", 0)

        user_content = f"Question: {q}\nExtracted Entities: {json.dumps(extracted)}"

        if reflection_feedback and retry_count > 0:
            user_content += f"\n\n--- PREVIOUS ATTEMPT FEEDBACK ---\nThe previous retrieval did not result in a satisfactory answer. \nFeedback from judge: {reflection_feedback}\nPlease refine the search plan to better address the user's question."

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]

        plan_data = {}
        usage_metadata = state.get("usage_metadata") or {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }
        try:
            chat = _get_chat_tool()
            response = chat.chat_completion(messages, temperature=0.0)
            res = response["content"]
            usage = response["usage"]

            # Accumulate usage
            for k in usage:
                usage_metadata[k] = usage_metadata.get(k, 0) + usage[k]

            json_match = re.search(r"\{.*\}", res, re.DOTALL)
            if json_match:
                plan_data = json.loads(json_match.group(0))
        except Exception as e:
            logger.warning(f"Planning LLM failed: {e}")

        # Construct final plan
        plan: RetrievalPlan = {
            "query_text": plan_data.get("query_text") or q,
            "top_k": plan_data.get("top_k", 20),
            "vector_k": 30,
            "extra_filter": plan_data.get("extra_filter"),
            "reason": plan_data.get("reason", "fallback"),
        }

        # Booster: if we have specific IDs, make sure they are in query_text
        all_ids = []
        for k in ["container", "po", "booking", "obl"]:
            all_ids.extend(extracted.get(k) or [])

        if all_ids:
            plan["query_text"] = " ".join(list(set(all_ids))) + " " + plan["query_text"]

        state["retrieval_plan"] = plan
        logger.info(f"Planned retrieval: {plan}")

        return state
