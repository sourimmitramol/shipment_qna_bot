import pandas as pd

from shipment_qna_bot.graph.nodes import analytics_planner as analytics_module


class _StubBlobManager:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def load_filtered_data(self, consignee_codes):
        return self._df.copy()

    def get_local_path(self):
        # We need a dummy path for duckdb to execute against a view. The actual test engine mock ignores it anyway.
        return "dummy.parquet"


class _StubChatTool:
    def __init__(self, code: str):
        self._code = code

    def chat_completion(self, messages, temperature=0.0):
        return {
            "content": f"```python\n{self._code}\n```",
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 1,
                "total_tokens": 2,
            },
        }


class _CapturingSqlChatTool:
    def __init__(self, sql: str):
        self._sql = sql
        self.messages = None

    def chat_completion(self, messages, temperature=0.0):
        self.messages = messages
        return {
            "content": f"```sql\n{self._sql}\n```",
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 1,
                "total_tokens": 2,
            },
        }


class _StubCon:
    def __init__(self):
        self.df = lambda: pd.DataFrame(
            {"discharge_port": ["LOS ANGELES"], "count": [12]}
        )

    def sql(self, *args, **kwargs):
        # returns self so `sample_rel.df()` can be called in the analytics code
        return self


class _StubEngine:
    def __init__(self, exec_result):
        self._exec_result = exec_result
        self.con = _StubCon()

    def execute_query(self, parquet_path, sql, consignee_codes):
        return dict(self._exec_result)


def _base_state(question: str):
    return {
        "question_raw": question,
        "normalized_question": question,
        "consignee_codes": ["0000866"],
        "intent": "analytics",
        "conversation_id": "chart-spec-test",
        "errors": [],
        "notices": [],
        "usage_metadata": {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        },
    }


def _exec_result():
    return {
        "success": True,
        "output": "",
        "result": "| discharge_port | count |\n|---|---|\n| LOS ANGELES | 12 |",
        "final_answer": "| discharge_port | count |\n|---|---|\n| LOS ANGELES | 12 |",
        "result_type": "DataFrame",
        "filtered_rows": None,
        "filtered_preview": "",
        "result_columns": ["discharge_port", "count"],
        "result_rows": [
            {"discharge_port": "LOS ANGELES", "count": 12},
            {"discharge_port": "NEW YORK", "count": 7},
            {"discharge_port": "HOUSTON", "count": 4},
        ],
    }


def _exec_result_dict():
    return {
        "success": True,
        "output": "",
        "result": "{'November 2025': 139, 'December 2025': 160}",
        "final_answer": "{'November 2025': 139, 'December 2025': 160}",
        "result_type": "dict",
        "filtered_rows": None,
        "filtered_preview": "",
        "result_columns": None,
        "result_rows": None,
        "result_value": {"November 2025": 139, "December 2025": 160},
    }


def test_analytics_generates_bar_chart_spec(monkeypatch):
    df = pd.DataFrame(
        {"discharge_port": ["LOS ANGELES"], "shipment_status": ["IN_OCEAN"]}
    )

    monkeypatch.setattr(analytics_module, "is_test_mode", lambda: False)
    monkeypatch.setattr(
        analytics_module, "_get_blob_manager", lambda: _StubBlobManager(df)
    )
    monkeypatch.setattr(
        analytics_module, "_get_chat", lambda: _StubChatTool("result = df")
    )
    monkeypatch.setattr(
        analytics_module, "_get_duckdb_engine", lambda: _StubEngine(_exec_result())
    )

    new_state = analytics_module.analytics_planner_node(
        _base_state("show bar chart of shipment count by discharge_port")
    )

    assert new_state["table_spec"] is not None
    assert new_state["table_spec"]["columns"] == ["discharge_port", "count"]
    assert len(new_state["table_spec"]["rows"]) == 3

    assert new_state["chart_spec"] is not None
    assert new_state["chart_spec"]["kind"] == "bar"
    assert new_state["chart_spec"]["encodings"]["x"] == "discharge_port"
    assert new_state["chart_spec"]["encodings"]["y"] == "count"


def test_analytics_generates_pie_chart_spec(monkeypatch):
    df = pd.DataFrame(
        {"discharge_port": ["LOS ANGELES"], "shipment_status": ["IN_OCEAN"]}
    )

    monkeypatch.setattr(analytics_module, "is_test_mode", lambda: False)
    monkeypatch.setattr(
        analytics_module, "_get_blob_manager", lambda: _StubBlobManager(df)
    )
    monkeypatch.setattr(
        analytics_module, "_get_chat", lambda: _StubChatTool("result = df")
    )
    monkeypatch.setattr(
        analytics_module, "_get_duckdb_engine", lambda: _StubEngine(_exec_result())
    )

    new_state = analytics_module.analytics_planner_node(
        _base_state("show pie chart of shipment count by discharge_port")
    )

    assert new_state["chart_spec"] is not None
    assert new_state["chart_spec"]["kind"] == "pie"
    assert new_state["chart_spec"]["encodings"]["label"] == "discharge_port"
    assert new_state["chart_spec"]["encodings"]["value"] == "count"


def test_analytics_keeps_table_without_chart_when_not_requested(monkeypatch):
    df = pd.DataFrame(
        {"discharge_port": ["LOS ANGELES"], "shipment_status": ["IN_OCEAN"]}
    )

    monkeypatch.setattr(analytics_module, "is_test_mode", lambda: False)
    monkeypatch.setattr(
        analytics_module, "_get_blob_manager", lambda: _StubBlobManager(df)
    )
    monkeypatch.setattr(
        analytics_module, "_get_chat", lambda: _StubChatTool("result = df")
    )
    monkeypatch.setattr(
        analytics_module, "_get_duckdb_engine", lambda: _StubEngine(_exec_result())
    )

    new_state = analytics_module.analytics_planner_node(
        _base_state("list shipment count by discharge_port")
    )

    assert new_state["table_spec"] is not None
    assert new_state.get("chart_spec") is None


def test_analytics_dict_result_generates_pie_chart(monkeypatch):
    df = pd.DataFrame(
        {"discharge_port": ["LOS ANGELES"], "shipment_status": ["IN_OCEAN"]}
    )

    monkeypatch.setattr(analytics_module, "is_test_mode", lambda: False)
    monkeypatch.setattr(
        analytics_module, "_get_blob_manager", lambda: _StubBlobManager(df)
    )
    monkeypatch.setattr(
        analytics_module,
        "_get_chat",
        lambda: _StubChatTool("result = {'November 2025': 139, 'December 2025': 160}"),
    )
    monkeypatch.setattr(
        analytics_module, "_get_duckdb_engine", lambda: _StubEngine(_exec_result_dict())
    )

    new_state = analytics_module.analytics_planner_node(
        _base_state(
            "show pie chart for containers received november 2025 vs december 2025"
        )
    )

    assert new_state["table_spec"] is not None
    assert new_state["table_spec"]["columns"] == ["label", "value"]
    assert len(new_state["table_spec"]["rows"]) == 2

    assert new_state["chart_spec"] is not None
    assert new_state["chart_spec"]["kind"] == "pie"
    assert new_state["chart_spec"]["encodings"]["label"] == "label"
    assert new_state["chart_spec"]["encodings"]["value"] == "value"


def test_unqualified_arrival_location_prompt_covers_dp_and_fd(monkeypatch):
    df = pd.DataFrame(
        {
            "discharge_port": ["LONG BEACH"],
            "final_destination": ["LONG BEACH"],
            "best_eta_dp_date": ["2026-04-10"],
        }
    )
    chat = _CapturingSqlChatTool("SELECT count(*) AS total FROM df")

    monkeypatch.setattr(analytics_module, "is_test_mode", lambda: False)
    monkeypatch.setattr(
        analytics_module, "_get_blob_manager", lambda: _StubBlobManager(df)
    )
    monkeypatch.setattr(analytics_module, "_get_chat", lambda: chat)
    monkeypatch.setattr(
        analytics_module, "_get_duckdb_engine", lambda: _StubEngine(_exec_result())
    )

    analytics_module.analytics_planner_node(
        _base_state("Show all POs scheduled to arrive at Long Beach in Apr 2026")
    )

    assert chat.messages is not None
    system_prompt = chat.messages[0]["content"]
    assert "treat that location as ambiguous and cover BOTH legs" in system_prompt
    assert "`discharge_port` OR `final_destination`" in system_prompt
    assert "COALESCE(best_eta_dp_date, eta_dp_date)" in system_prompt
    assert "COALESCE(best_eta_fd_date, eta_fd_date)" in system_prompt
