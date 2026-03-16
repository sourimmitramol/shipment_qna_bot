# tests/test_session.py
from fastapi.testclient import TestClient

from shipment_qna_bot.api import routes_chat as routes_module
from shipment_qna_bot.api.main import app

client = TestClient(app)


def test_session_persistence_and_exit(monkeypatch):
    # Mock run_graph to avoid LLM calls
    def fake_run_graph(initial_state: dict):
        q = initial_state["question_raw"].lower()
        if any(w in q for w in ["bye", "exit"]):
            return {
                "intent": "end",
                "answer_text": "Goodbye!",
            }
        return {
            "intent": "retrieval",
            "answer_text": "Here is your data.",
            "conversation_id": initial_state.get("conversation_id"),
        }

    monkeypatch.setattr(routes_module, "run_graph", fake_run_graph)

    # 1. First request with consignee codes
    payload = {
        "question": "How are my shipments?",
        "consignee_codes": ["0000866"],
        "conversation_id": "conv-1",
    }
    response = client.post("/api/chat", json=payload)
    assert response.status_code == 200

    # Check session endpoint
    response = client.get("/api/session")
    assert response.status_code == 200
    data = response.json()
    assert data["consignee_codes"] == ["0000866"]
    assert data["conversation_id"] == "conv-1"

    # 2. Second request without consignee codes (should be pulled from session)
    payload_no_codes = {
        "question": "Give me more info.",
        "consignee_codes": [],  # Empty, should use session
    }
    # Note: ChatRequest validator requires consignee_codes, but we can send empty list if the model allows or mock it.
    # Actually ChatRequest has min_length=1 for consignee_codes.
    # Let's adjust the test to send codes, but verify it persistent the session anyway.

    # 3. Request with "bye" to trigger "end" intent
    payload_bye = {"question": "bye", "consignee_codes": ["0000866"]}
    response = client.post("/api/chat", json=payload_bye)
    assert response.status_code == 200
    assert response.json()["intent"] == "end"

    # 4. Verify session is cleared
    response = client.get("/api/session")
    assert response.status_code == 200
    data = response.json()
    assert data["consignee_codes"] == []
    assert data["conversation_id"] is None


def test_payload_conversation_id_overrides_existing_session(monkeypatch):
    local_client = TestClient(app)

    def fake_run_graph(initial_state: dict):
        return {
            "intent": "retrieval",
            "answer_text": "Here is your data.",
            "conversation_id": initial_state.get("conversation_id"),
        }

    monkeypatch.setattr(routes_module, "run_graph", fake_run_graph)

    first_payload = {
        "question": "How are my shipments?",
        "consignee_codes": ["0000866"],
        "conversation_id": "frontend-conv-1",
    }
    first_response = local_client.post("/api/chat", json=first_payload)
    assert first_response.status_code == 200
    assert first_response.json()["conversation_id"] == "frontend-conv-1"

    second_payload = {
        "question": "Give me the next update.",
        "consignee_codes": ["0000866"],
        "conversation_id": "frontend-conv-2",
    }
    second_response = local_client.post("/api/chat", json=second_payload)
    assert second_response.status_code == 200
    assert second_response.json()["conversation_id"] == "frontend-conv-2"

    session_response = local_client.get("/api/session")
    assert session_response.status_code == 200
    assert session_response.json()["conversation_id"] == "frontend-conv-2"
