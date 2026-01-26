# tests/test_route_chat.py

from fastapi.testclient import TestClient

from shipment_qna_bot.api import routes_chat as routes_module
from shipment_qna_bot.api.main import app  # adjust if your app is elsewhere

client = TestClient(app)


def test_chat_endpoint_basic_flow(monkeypatch):
    """
    Sanity check:
    - /api/chat accepts payload with comma-packed consignee_codes
    - resolve_allowed_scope is applied (on the route module symbol)
    - run_graph result is mapped into ChatAnswer
    """

    # --- monkeypatch resolve_allowed_scope as seen by routes_chat ---
    def fake_resolve_allowed_scope(user_identity, payload_codes):
        # For this test, we simulate a restriction:
        # even if caller sends two codes, only the first is allowed.
        assert payload_codes == ["0000866", "234567"]
        return ["0000866"]

    monkeypatch.setattr(
        routes_module, "resolve_allowed_scope", fake_resolve_allowed_scope
    )

    # --- monkeypatch run_graph as seen by routes_chat ---
    def fake_run_graph(initial_state: dict):
        # Here we inspect what the route actually passes.
        assert initial_state["question_raw"].startswith("Show me")
        # Crucial: should be using allowed scope, not raw payload
        assert initial_state["consignee_codes"] == ["0000866"]
        # Return a deterministic fake state the route can map
        return {
            "intent": "status",
            "answer_text": "Stubbed answer from fake graph.",
            "notices": ["This is a fake graph result."],
            "citations": [
                {
                    "doc_id": "320001075737",
                    "container_number": "TIIU5855662",
                    "field_used": ["etd_lp_date"],
                }
            ],
            "chart_spec": {
                "kind": "bar",
                "title": "Fake chart",
                "data": [{"status": "ON_TIME", "count": 1}],
                "encodings": {"x": "status", "y": "count"},
            },
            "table_spec": {
                "columns": ["status", "count"],
                "rows": [{"status": "ON_TIME", "count": 1}],
                "title": "Fake table",
            },
        }

    monkeypatch.setattr(routes_module, "run_graph", fake_run_graph)

    # --- Call the API ---
    payload = {
        "question": "Show me the status for all my open containers",
        "consignee_codes": ["0000866,234567"],  # comma-packed in list
        "conversation_id": "test-conv-123",
    }

    resp = client.post("/api/chat", json=payload)
    assert resp.status_code == 200

    data = resp.json()

    # --- Basic checks on the response shape ---
    assert data["conversation_id"] == "test-conv-123"
    # intent should come from our fake_run_graph
    assert data["intent"] == "status"
    assert data["answer"].startswith("Stubbed answer")

    # Notices mapping
    assert data["notices"] == ["This is a fake graph result."]

    # Evidence mapping
    assert data["evidence"]
    assert data["evidence"][0]["doc_id"] == "320001075737"
    assert data["evidence"][0]["container_number"] == "TIIU5855662"

    # Chart & table mapping
    assert data["chart"] is not None
    assert data["chart"]["kind"] == "bar"
    assert data["chart"]["data"][0]["status"] == "ON_TIME"

    assert data["table"] is not None
    assert data["table"]["rows"][0]["status"] == "ON_TIME"
