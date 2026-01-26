import os

import pytest

from shipment_qna_bot.graph.builder import run_graph


def _has_live_env() -> bool:
    required = [
        "AZURE_OPENAI_API_KEY",
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_API_VERSION",
        "AZURE_OPENAI_EMBED_DEPLOYMENT",
        "AZURE_SEARCH_ENDPOINT",
        "AZURE_SEARCH_API_KEY",
        "AZURE_SEARCH_INDEX_NAME",
    ]
    return all(os.getenv(k) for k in required)


@pytest.mark.skipif(
    os.getenv("SHIPMENT_QNA_BOT_RUN_INTEGRATION") != "1",
    reason="Set SHIPMENT_QNA_BOT_RUN_INTEGRATION=1 to run live integration tests.",
)
def test_live_graph_smoke(monkeypatch: pytest.MonkeyPatch) -> None:
    if not _has_live_env():
        pytest.skip("Live env vars not set for Azure OpenAI/Search.")

    monkeypatch.setenv("SHIPMENT_QNA_BOT_TEST_MODE", "0")

    result = run_graph(
        {
            "conversation_id": "integration-test",
            "question_raw": "What is the ETA for container ABCD1234567?",
            "consignee_codes": ["TEST"],
        }
    )

    assert isinstance(result, dict)
    assert "intent" in result
