from pathlib import Path


def test_structured_weather_payloads_are_not_hard_gated_to_analytics_intent():
    html = Path("src/shipment_qna_bot/static/index.html").read_text(encoding="utf-8")

    assert "const hasStructuredPayload =" in html
    assert (
        "const allowStructuredRender = isAnalyticsIntent || hasStructuredPayload;"
        in html
    )
    assert (
        "if (allowStructuredRender && chart && Array.isArray(chart.data) && chart.data.length > 0)"
        in html
    )
    assert (
        "if (allowStructuredRender && table && table.rows && table.rows.length > 0)"
        in html
    )
