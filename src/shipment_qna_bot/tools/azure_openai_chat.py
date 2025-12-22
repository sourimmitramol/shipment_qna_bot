import os
from typing import Any, Dict, List, Optional

from dotenv import find_dotenv, load_dotenv
from openai import AzureOpenAI

# Load environment variables
load_dotenv(find_dotenv(), override=True)


class AzureOpenAIChatTool:
    def __init__(self):
        self.api_key = os.getenv("AZURE_OPENAI_API_KEY")
        self.api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")

        # Support multiple naming conventions
        self.azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT") or os.getenv(
            "ENDPOINT_URL"
        )
        self.deployment_name = (
            os.getenv("AZURE_OPENAI_DEPLOYMENT")
            or os.getenv("DEPLOYMENT_NAME")
            or "gpt-4o"
        )

        if not self.api_key or not self.azure_endpoint or not self.deployment_name:
            # Debug info to help user if it still fails
            missing = []
            if not self.api_key:
                missing.append("AZURE_OPENAI_API_KEY")
            if not self.azure_endpoint:
                missing.append("AZURE_OPENAI_ENDPOINT/ENDPOINT_URL")
            if not self.deployment_name:
                missing.append("AZURE_OPENAI_DEPLOYMENT/DEPLOYMENT_NAME")

            raise ValueError(
                f"Missing Azure OpenAI credentials: {', '.join(missing)}. "
                "Please check your .env file."
            )

        self.client = AzureOpenAI(
            api_key=self.api_key,
            api_version=self.api_version,
            azure_endpoint=self.azure_endpoint,
        )

    def chat_completion(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.0,
        max_tokens: int = 800,
    ) -> Dict[str, Any]:
        """
        Generates a chat completion using the Azure OpenAI client.
        Returns a dict with 'content' and 'usage'.
        """
        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            return {
                "content": response.choices[0].message.content or "",
                "usage": {
                    "prompt_tokens": response.usage.prompt_tokens,
                    "completion_tokens": response.usage.completion_tokens,
                    "total_tokens": response.usage.total_tokens,
                },
            }
        except Exception as e:
            raise RuntimeError(f"Azure OpenAI Chat Completion failed: {e}")
