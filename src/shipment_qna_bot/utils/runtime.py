import os


def is_test_mode() -> bool:
    flag = os.getenv("SHIPMENT_QNA_BOT_TEST_MODE")
    if flag is not None:
        return flag.strip().lower() in {"1", "true", "yes", "on"}
    return bool(os.getenv("PYTEST_CURRENT_TEST"))
