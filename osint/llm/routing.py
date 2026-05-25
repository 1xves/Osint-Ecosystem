"""
osint/llm/routing.py

Deterministic model routing for the OSINT pipeline.

Model selection is NEVER made by an LLM at runtime.
Every task type maps to exactly one model, defined in MODEL_ROUTING (config.py).
This is a deliberate design constraint — dynamic routing by LLM creates
unpredictable cost and behavior.

Usage:
    from osint.llm.routing import get_model_for_task, LLMRouter

    # Simple lookup
    model = get_model_for_task("structured_extraction_clean")
    # → "qwen3:7b"

    # Full router with LLM client
    router = LLMRouter(ollama_client)
    response = await router.call("structured_extraction_clean", prompt, system)
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.config import MODEL_ROUTING, settings
from osint.llm.ollama import OllamaClient, OllamaError

log = logging.getLogger(__name__)


def get_model_for_task(task_type: str) -> str:
    """
    Return the model name for a task type.

    Args:
        task_type: One of the keys in MODEL_ROUTING (see config.py).

    Returns:
        Model name string (e.g., "qwen3:14b").
        Falls back to MODEL_ROUTING["default"] if task_type not found.
    """
    model = MODEL_ROUTING.get(task_type)
    if model is None:
        log.warning(
            "get_model_for_task: unknown task_type '%s', falling back to default (%s)",
            task_type,
            MODEL_ROUTING["default"],
        )
        model = MODEL_ROUTING["default"]
    return model


# ─────────────────────────────────────────────────────────────────────────────
# Task type constants
# These are the valid task_type values. Only add new ones here AND in config.py.
# ─────────────────────────────────────────────────────────────────────────────

class TaskType:
    """Valid task types for model routing."""
    STRUCTURED_EXTRACTION_CLEAN = "structured_extraction_clean"
    STRUCTURED_EXTRACTION_TEXT  = "structured_extraction_text"
    ENTITY_RESOLUTION           = "entity_resolution_arbitration"
    FRAMING_GENERATION          = "framing_generation"
    RELATIONSHIP_INFERENCE      = "relationship_inference"
    ENTITY_SCORING              = "entity_scoring"
    CLAIM_VERIFICATION          = "claim_verification"
    BRIEF_DRAFTING              = "brief_drafting"
    BRIEF_POLISH                = "brief_polish"
    GAP_ANALYSIS                = "gap_analysis"
    DEFAULT                     = "default"

    # ── Document extraction ───────────────────────────────────────────────────
    # These task types are used by DocumentExtractor (osint/llm/extractors.py).
    # Each maps to the appropriate model via MODEL_ROUTING in config.py.
    DOCUMENT_EXTRACTION_PROXY  = "document_extraction_proxy"   # SEC DEF 14A / proxy statements
    DOCUMENT_EXTRACTION_10K    = "document_extraction_10k"     # SEC 10-K annual reports
    DOCUMENT_EXTRACTION_COURT  = "document_extraction_court"   # Court filings / dockets
    DOCUMENT_EXTRACTION_990    = "document_extraction_990"     # IRS Form 990 XML sections


class LLMRouter:
    """
    Routes LLM calls to the correct model based on task type.
    Wraps OllamaClient with deterministic task → model dispatch.

    One instance per worker, shared across all agents.
    """

    def __init__(self, client: OllamaClient) -> None:
        self._client = client

    async def call(
        self,
        task_type: str,
        prompt: str,
        system: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> dict[str, Any]:
        """
        Make a generation call routed to the correct model.

        Args:
            task_type: One of the TaskType constants.
            prompt: The user prompt.
            system: System prompt (optional).
            temperature: Override model default (use sparingly).
            max_tokens: Override model default (use sparingly).

        Returns:
            OllamaClient.generate() dict: {text, tokens_in, tokens_out, done, context}
        """
        model = get_model_for_task(task_type)
        log.debug("LLMRouter: task=%s → model=%s", task_type, model)
        return await self._client.generate(
            model=model,
            prompt=prompt,
            system=system,
            temperature=temperature,
            max_tokens=max_tokens,
        )

    async def call_json(
        self,
        task_type: str,
        prompt: str,
        system: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """
        Make a JSON generation call routed to the correct model.

        Returns:
            (parsed_json, generation_metadata)
        """
        model = get_model_for_task(task_type)
        log.debug("LLMRouter: task=%s (json) → model=%s", task_type, model)
        return await self._client.generate_json(
            model=model,
            prompt=prompt,
            system=system,
            temperature=temperature,
            max_tokens=max_tokens,
        )

    async def embed(self, text: str) -> list[float]:
        """
        Generate an embedding using the configured embed model.
        Always uses ollama_embed_model — never routed differently.
        """
        return await self._client.embed(text)

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return await self._client.embed_batch(texts)

    async def verify_models(self) -> dict[str, bool]:
        """
        Check that all models required by MODEL_ROUTING are available.
        Returns {model_name: is_available} for each unique model.

        Call at startup to surface missing models early.
        """
        required_models = set(MODEL_ROUTING.values())
        required_models.add(settings.ollama_embed_model)

        results = {}
        for model in required_models:
            available = await self._client.is_model_available(model)
            results[model] = available
            if not available:
                log.warning(
                    "LLMRouter.verify_models: model '%s' is NOT available in Ollama. "
                    "Run: ollama pull %s",
                    model, model,
                )
        return results
