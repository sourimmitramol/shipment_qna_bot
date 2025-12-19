import re
from typing import Dict, List

from shipment_qna_bot.graph.state import GraphState
from shipment_qna_bot.logging.logger import logger


def extractor_node(state: GraphState) -> GraphState:
    """
    Extracts entities (Container, PO, OBL) from the normalized question.
    """
    text = state.get("normalized_question", "")

    # Regex patterns (simplified for demo)
    # Container: 4 letters + 7 digits (e.g., SEGU5935510)
    container_pattern = r"\b[a-zA-Z]{4}\d{7}\b"
    # PO: 10 digits OR Prefix PO + digits
    po_pattern = r"\b(?:PO)?(\d{10})\b"
    # OBL: Prefix OBL + alphanumeric (up to 20 chars)
    obl_pattern = r"\b(?:OBL)?([a-zA-Z0-9]{5,20})\b"

    containers = [c.upper() for c in re.findall(container_pattern, text)]
    pos = [p.upper() for p in re.findall(po_pattern, text, re.IGNORECASE)]
    obls = [o.upper() for o in re.findall(obl_pattern, text, re.IGNORECASE)]

    extracted = {
        "container": containers,
        "po": pos,
        "obl": obls,
    }

    count = sum(len(v) for v in extracted.values())
    logger.info(
        f"Extracted {count} entities", extra={"extra_data": {"extracted": extracted}}
    )

    return {"extracted_ids": extracted}
