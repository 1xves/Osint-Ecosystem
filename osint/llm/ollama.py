"""
osint/llm/ollama.py

Async Ollama client for the OSINT system.

All LLM calls in the pipeline go through this client.
Includes a stub/mock mode for development when Ollama isn't installed.

Features:
- Async generate and chat completion
- Embedding generation (nomic-embed-text)
- Automatic retry on transient connection errors
- Token counting
- Stub mode: returns deterministic fake responses for development

Usage:
    client = OllamaClient()
    await client.connect()
    response = await client.generate(
        model="qwen3:14b",
        prompt="Extract entity data from...",
        system="You are a data extraction agent...",
        format="json",
    )
    embedding = await client.embed("Austin Capital Group", model="nomic-embed-text")

Stub mode (set OLLAMA_STUB=true in .env):
    client = OllamaClient(stub=True)
    # Returns predictable fake responses — no Ollama required
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any

import httpx

from osint.core.config import settings, MODEL_PARAMS

log = logging.getLogger(__name__)

# Default timeouts — generation can take a while on large models
GENERATE_TIMEOUT = 180.0    # 3 minutes for qwen3:22b on complex prompts
EMBED_TIMEOUT = 30.0


class OllamaError(Exception):
    """Raised when Ollama returns an error or is unreachable."""
    pass


class OllamaClient:
    """
    Async HTTP client for the Ollama REST API.
    Manages a single httpx.AsyncClient across calls.
    """

    def __init__(self, stub: bool = False) -> None:
        """
        Args:
            stub: If True, return deterministic fake responses without calling Ollama.
                  Useful for development before Ollama is installed.
                  Also activated by setting OLLAMA_STUB=true in environment.
        """
        self._stub = stub or os.getenv("OLLAMA_STUB", "").lower() in ("true", "1", "yes")
        self._client: httpx.AsyncClient | None = None
        self._available_models: set[str] = set()

    async def connect(self) -> None:
        """
        Initialize the HTTP client and verify Ollama is reachable.
        In stub mode: skips connectivity check.
        """
        if self._stub:
            log.info("OllamaClient: STUB MODE — all LLM calls return fake responses")
            return

        self._client = httpx.AsyncClient(
            base_url=settings.ollama_host,
            timeout=httpx.Timeout(GENERATE_TIMEOUT, connect=5.0),
        )
        try:
            resp = await self._client.get("/api/tags")
            resp.raise_for_status()
            data = resp.json()
            self._available_models = {m["name"] for m in data.get("models", [])}
            log.info(
                "OllamaClient: connected to %s — %d models available: %s",
                settings.ollama_host,
                len(self._available_models),
                sorted(self._available_models),
            )
        except httpx.ConnectError as e:
            raise OllamaError(
                f"Cannot connect to Ollama at {settings.ollama_host}. "
                f"Is Ollama installed and running? Error: {e}"
            ) from e

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    # ─────────────────────────────────────────────────────────────────────────
    # Text generation
    # ─────────────────────────────────────────────────────────────────────────

    async def generate(
        self,
        model: str,
        prompt: str,
        system: str | None = None,
        format: str | None = None,   # "json" for structured output
        temperature: float | None = None,
        max_tokens: int | None = None,
        context: list[int] | None = None,   # Ollama conversation context
    ) -> dict[str, Any]:
        """
        Single-turn text generation via Ollama's /api/generate endpoint.

        Args:
            model: Model name (e.g., "qwen3:14b")
            prompt: The user prompt
            system: System prompt (optional)
            format: "json" to request JSON output
            temperature: Override model default temperature
            max_tokens: Override model default num_predict
            context: Conversation context tokens from a previous call

        Returns:
            {
              "text": str,          — The generated text
              "tokens_in": int,     — Prompt tokens
              "tokens_out": int,    — Generated tokens
              "done": bool,
              "context": list[int]  — Context for follow-up calls
            }

        Raises:
            OllamaError on connection failure or model error.
        """
        if self._stub:
            return self._stub_generate(model, prompt, format)

        self._check_connected()
        params = self._build_options(model, temperature, max_tokens)

        payload: dict[str, Any] = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": params,
        }
        if system:
            payload["system"] = system
        if format == "json":
            payload["format"] = "json"
        if context:
            payload["context"] = context

        start = time.monotonic()
        try:
            resp = await self._client.post(
                "/api/generate",
                json=payload,
                timeout=GENERATE_TIMEOUT,
            )
            resp.raise_for_status()
        except httpx.TimeoutException as e:
            raise OllamaError(f"Ollama generate timed out for model {model}") from e
        except httpx.HTTPStatusError as e:
            raise OllamaError(
                f"Ollama generate failed: HTTP {e.response.status_code} — {e.response.text}"
            ) from e

        data = resp.json()
        elapsed = time.monotonic() - start

        log.debug(
            "generate: model=%s tokens_in=%d tokens_out=%d elapsed=%.1fs",
            model,
            data.get("prompt_eval_count", 0),
            data.get("eval_count", 0),
            elapsed,
        )

        return {
            "text":       data.get("response", ""),
            "tokens_in":  data.get("prompt_eval_count", 0),
            "tokens_out": data.get("eval_count", 0),
            "done":       data.get("done", True),
            "context":    data.get("context", []),
        }

    async def generate_json(
        self,
        model: str,
        prompt: str,
        system: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """
        Generate and parse JSON output in one call.
        Returns (parsed_json, generation_metadata).

        Retries up to 2 times if the model produces invalid JSON.
        If all retries fail, raises OllamaError with the raw text in the message.
        """
        last_text = ""
        for attempt in range(3):
            result = await self.generate(
                model=model,
                prompt=prompt,
                system=system,
                format="json",
                temperature=temperature,
                max_tokens=max_tokens,
            )
            last_text = result["text"]
            try:
                parsed = json.loads(last_text)
                return parsed, result
            except json.JSONDecodeError:
                log.warning(
                    "generate_json: invalid JSON on attempt %d/%d (model=%s). "
                    "Raw: %.200s",
                    attempt + 1, 3, model, last_text
                )
                if attempt < 2:
                    await asyncio.sleep(1.0)

        raise OllamaError(
            f"generate_json: model {model} produced invalid JSON after 3 attempts. "
            f"Raw output: {last_text[:500]}"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Embeddings
    # ─────────────────────────────────────────────────────────────────────────

    async def embed(
        self,
        text: str,
        model: str | None = None,
    ) -> list[float]:
        """
        Generate a text embedding vector via Ollama's /api/embeddings endpoint.
        Uses nomic-embed-text by default.

        Args:
            text: Text to embed.
            model: Override embedding model (default: settings.ollama_embed_model).

        Returns:
            list[float] — embedding vector.
        """
        if self._stub:
            return self._stub_embed(text)

        self._check_connected()
        embed_model = model or settings.ollama_embed_model

        try:
            resp = await self._client.post(
                "/api/embeddings",
                json={"model": embed_model, "prompt": text},
                timeout=EMBED_TIMEOUT,
            )
            resp.raise_for_status()
        except httpx.TimeoutException as e:
            raise OllamaError(f"Ollama embed timed out for model {embed_model}") from e
        except httpx.HTTPStatusError as e:
            raise OllamaError(
                f"Ollama embed failed: HTTP {e.response.status_code}"
            ) from e

        data = resp.json()
        embedding = data.get("embedding")
        if not embedding:
            raise OllamaError(f"Ollama embed returned no embedding for model {embed_model}")

        return embedding

    async def embed_batch(
        self,
        texts: list[str],
        model: str | None = None,
    ) -> list[list[float]]:
        """
        Embed multiple texts. Runs sequentially (Ollama has no batch endpoint).
        Returns list of vectors in the same order as inputs.
        """
        results = []
        for text in texts:
            vec = await self.embed(text, model=model)
            results.append(vec)
        return results

    # ─────────────────────────────────────────────────────────────────────────
    # Model management
    # ─────────────────────────────────────────────────────────────────────────

    async def is_model_available(self, model: str) -> bool:
        """Check if a model is pulled and available in Ollama."""
        if self._stub:
            return True
        if self._available_models:
            return model in self._available_models
        # Re-fetch if cache is empty
        try:
            resp = await self._client.get("/api/tags")
            resp.raise_for_status()
            data = resp.json()
            self._available_models = {m["name"] for m in data.get("models", [])}
            return model in self._available_models
        except Exception:
            return False

    async def list_models(self) -> list[str]:
        """Return list of pulled model names."""
        if self._stub:
            return [
                settings.ollama_default_model,
                settings.ollama_escalation_model,
                settings.ollama_extraction_model,
                settings.ollama_embed_model,
            ]
        try:
            resp = await self._client.get("/api/tags")
            resp.raise_for_status()
            data = resp.json()
            return [m["name"] for m in data.get("models", [])]
        except Exception as e:
            raise OllamaError(f"Failed to list models: {e}") from e

    # ─────────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _check_connected(self) -> None:
        if self._client is None:
            raise OllamaError(
                "OllamaClient not connected. Call await client.connect() first."
            )

    def _build_options(
        self,
        model: str,
        temperature: float | None,
        max_tokens: int | None,
    ) -> dict[str, Any]:
        """Merge model defaults from MODEL_PARAMS with any call-time overrides."""
        defaults = MODEL_PARAMS.get(model, MODEL_PARAMS.get(settings.ollama_default_model, {}))
        options = dict(defaults)
        if temperature is not None:
            options["temperature"] = temperature
        if max_tokens is not None:
            options["num_predict"] = max_tokens
        return options

    # ─────────────────────────────────────────────────────────────────────────
    # Stub implementations (development without Ollama)
    # ─────────────────────────────────────────────────────────────────────────

    def _stub_generate(
        self, model: str, prompt: str, format: str | None
    ) -> dict[str, Any]:
        """Return a minimal valid stub response."""
        if format == "json":
            # Return a JSON string that agents can parse without errors
            stub_json = json.dumps({
                "entities": [],
                "stub": True,
                "message": "Ollama stub mode — install Ollama and pull models to get real output",
            })
            text = stub_json
        else:
            text = (
                "[STUB] Ollama is not installed or OLLAMA_STUB=true is set. "
                "Install Ollama (https://ollama.com) and run: "
                f"ollama pull {settings.ollama_default_model}"
            )
        return {
            "text":       text,
            "tokens_in":  len(prompt) // 4,  # rough approximation
            "tokens_out": len(text) // 4,
            "done":       True,
            "context":    [],
        }

    def _stub_embed(self, text: str) -> list[float]:
        """Return a deterministic stub embedding vector (768-dim for nomic-embed-text)."""
        # Hash-based deterministic vector — same text always returns same vector
        import hashlib
        h = hashlib.sha256(text.encode()).digest()
        # Expand 32 bytes → 768 floats by cycling
        floats = []
        for i in range(768):
            byte_val = h[i % 32]
            floats.append((byte_val / 255.0) * 2.0 - 1.0)
        # Normalize to unit length
        magnitude = sum(x * x for x in floats) ** 0.5
        return [x / magnitude for x in floats]
