import os

import pytest


@pytest.fixture(autouse=True)
def _force_test_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    if os.getenv("SHIPMENT_QNA_BOT_RUN_INTEGRATION") == "1":
        monkeypatch.setenv("SHIPMENT_QNA_BOT_TEST_MODE", "0")
    else:
        monkeypatch.setenv("SHIPMENT_QNA_BOT_TEST_MODE", "1")
