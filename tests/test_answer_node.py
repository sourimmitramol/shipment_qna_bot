from shipment_qna_bot.graph.nodes import answer as answer_module


class _StubChatTool:
    def chat_completion(self, messages, temperature=0.0):
        return {
            "content": "I found matching shipments.",
            "usage": {
                "prompt_tokens": 3,
                "completion_tokens": 2,
                "total_tokens": 5,
            },
        }


def _base_state():
    return {
        "conversation_id": "answer-node-test",
        "consignee_codes": ["0000866"],
        "intent": "retrieval",
        "question_raw": "What is status of 6300150977",
        "today_date": "2026-03-11",
        "messages": [],
        "usage_metadata": {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        },
        "idx_analytics": {"count": 3, "facets": None},
        "extracted_ids": {
            "container_number": [],
            "po_numbers": ["6300150977"],
            "booking_numbers": [],
            "obl_nos": [],
        },
        "hits": [
            {
                "container_number": "ABCD1234567",
                "po_numbers": ["6300150977"],
                "shipment_status": "IN_OCEAN",
                "discharge_port": "LOS ANGELES",
                "eta_dp_date": "2026-03-20T00:00:00+00:00",
            },
            {
                "container_number": "EFGH1234567",
                "po_numbers": ["6300150977"],
                "shipment_status": "AT_DISCHARGE_PORT",
                "discharge_port": "LOS ANGELES",
                "eta_dp_date": "2026-03-18T00:00:00+00:00",
            },
            {
                "container_number": "IJKL1234567",
                "po_numbers": ["6300150977"],
                "shipment_status": "DELIVERED",
                "discharge_port": "LOS ANGELES",
                "eta_dp_date": "2026-03-15T00:00:00+00:00",
            },
        ],
    }


def test_answer_node_includes_associated_container_numbers(monkeypatch):
    monkeypatch.setattr(answer_module, "is_test_mode", lambda: False)
    monkeypatch.setattr(answer_module, "_get_chat_tool", lambda: _StubChatTool())
    monkeypatch.setattr(answer_module, "load_ready_ref", lambda: "")

    new_state = answer_module.answer_node(_base_state())
    answer_text = new_state.get("answer_text") or ""

    assert "Associated container numbers (3):" in answer_text
    assert "ABCD1234567" in answer_text
    assert "EFGH1234567" in answer_text
    assert "IJKL1234567" in answer_text
    assert "Showing 10 of 3 below." not in answer_text

    table_spec = new_state.get("table_spec")
    assert table_spec is not None
    assert len(table_spec["rows"]) == 3
