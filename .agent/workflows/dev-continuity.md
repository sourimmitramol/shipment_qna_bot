---
description: Procedures for maintaining the Shipment QnA Bot logic (IER Framework)
---

# Development Continuity Playbook

Whenever you are tasked with modifying the `shipment_qna_bot` codebase, you MUST follow this IER (Intent-Execution-Result) framework to ensure compatibility.

## 1. PRE-IMPLEMENTATION AUDIT
Before writing any code, verify:
- [ ] Is this search or analytics?
- [ ] If analytics, check `src/shipment_qna_bot/tools/analytics_metadata.py` for column names.
- [ ] If search, check `src/shipment_qna_bot/graph/nodes/retrieve.py` for `_FILTER_FIELDS`.

## 2. THE IER CONSTRAINTS

### INTENT: Classification Guardrails
- Queries with specific Shipment/Container IDs -> **Search Path**.
- Queries requiring aggregations/math -> **Analytics Path**.
- Abrupt topic shifts must trigger a context reset in the `normalizer.py`.

### EXECUTION: Code Generation Rules
- **OData (Search)**: Absolute prohibition of date math (`now()`, `add`).
- **Pandas (Analytics)**: Must use `result = ...` assignment. No global side effects.
- **Delay Logic**: Default to discharge port (`dp_delayed_dur`).

### RESULT: Presentation & Grounding
- All numbers must be grounded in the JSON hits or Pandas dataframes.
- Use markdown tables for multiple-row outputs.

## 3. VERIFICATION PROTOCOL
- Run `pytest tests/test_pandas_flow.py` for analytics changes.
- Run `pytest tests/test_integration_live.py` for retrieval changes.
