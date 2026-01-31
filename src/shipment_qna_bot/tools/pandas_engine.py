import contextlib
import io
import json
import sys
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from shipment_qna_bot.logging.logger import logger


class PandasAnalyticsEngine:
    """
    Safely executes Python/Pandas code on a provided DataFrame.
    Use this to perform aggregations, filtering, and detailed analysis that
    vector search cannot handle (e.g., "average weight", "count delays by port").
    """

    def __init__(self):
        # Allow specific safe modules to be used in the exec environment
        self.allowed_modules = {
            "pd": pd,
            "pandas": pd,
            "np": np,
            "numpy": np,
            "json": json,
        }

    def execute_code(self, df: pd.DataFrame, code: str) -> Dict[str, Any]:
        """
        Executes the provided Python code with the DataFrame `df` in context.
        The user code should print the result or assign it to a variable named `result`.

        Returns:
            Dict containing:
            - 'output': Captured stdout (print statements)
            - 'result': Value of 'result' variable if defined
            - 'error': Error message if failed
            - 'success': Bool
        """
        logger.info(f"Pandas Engine executing code on DF with shape {df.shape}")
        logger.info(f"Pandas Code:\n{code}")

        # Trap stdout
        output_buffer = io.StringIO()

        # Execution context
        local_scope = {
            "df": df,
            "pd": pd,
            "np": np,
            "json": json,
            "result": None,  # User code can assign to this
        }

        try:
            with contextlib.redirect_stdout(output_buffer):
                exec(code, {}, local_scope)

            output = output_buffer.getvalue()
            result_val = local_scope.get("result")

            # If result is a dataframe or series, convert to something json-serializable/string
            # for the agent to consume easily
            if isinstance(result_val, (pd.DataFrame, pd.Series)):
                result_export = result_val.to_markdown()
            else:
                result_export = str(result_val) if result_val is not None else ""

            # If no result variable, rely on print output
            final_answer = result_export if result_export else output

            return {
                "success": True,
                "output": output,
                "result": result_export,
                "final_answer": final_answer.strip(),
            }

        except Exception as e:
            logger.error(f"Pandas execution failed: {e}")
            return {
                "success": False,
                "error": f"{type(e).__name__}: {str(e)}",
                "output": output_buffer.getvalue(),
            }
