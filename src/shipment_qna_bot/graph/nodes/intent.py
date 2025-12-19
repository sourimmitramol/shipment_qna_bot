from shipment_qna_bot.graph.state import GraphState
from shipment_qna_bot.logging.logger import logger
from shipment_qna_bot.tools.azure_openai_chat import AzureOpenAIChatTool

_CHAT_TOOL = None


def _get_chat_tool() -> AzureOpenAIChatTool:
    global _CHAT_TOOL
    if _CHAT_TOOL is None:
        _CHAT_TOOL = AzureOpenAIChatTool()
    return _CHAT_TOOL


def intent_node(state: GraphState) -> GraphState:
    """
    Classifies the user's intent using LLM.
    """
    text = state.get("normalized_question", "")
    if not text:
        return {"intent": "end"}

    system_prompt = (
        "You are an intent classifier for a Shipment Q&A Bot. "
        "Classify the user's question into ONE of these categories: "
        "- 'retrieval': Questions about container status, ETA, delays, or specific shipment details. "
        "- 'analytics': Questions asking for summaries, charts, table aggregations, or cross-shipment insights. "
        "- 'greeting': Just saying hello, hi, or general non-shipment small talk. "
        "- 'end': Gibberish or completely irrelevant topics. "
        "Output ONLY the category name in lowercase."
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": text},
    ]

    try:
        chat_tool = _get_chat_tool()
        intent = chat_tool.chat_completion(messages, temperature=0.0).strip().lower()

        # Valid intents: retrieval, analytics, greeting, end
        valid_intents = ["retrieval", "analytics", "greeting", "end"]
        if intent not in valid_intents:
            # Fallback for shipment-like content
            intent = "retrieval"
    except Exception as e:
        logger.error(f"Intent classification failed: {e}")
        intent = "retrieval"  # Safe fallback

    logger.info(
        f"Classified intent: {intent}",
        extra={"extra_data": {"text_snippet": text[:50]}},
    )

    return {"intent": intent}
