"""
osint/llm/extractors.py

LLM-powered document extraction layer for the OSINT pipeline.

Extracts structured data from unstructured documents (PDFs, HTML, XML) using
targeted LLM prompts. Each document type has its own system prompt tuned for
the specific fields needed.

Dispatch pattern:
    DocumentExtractor.extract(text, document_type, entity_context)
    → dispatches to the correct TaskType based on document_type string
    → returns a structured dict with extracted fields

Supported document types:
    "proxy_statement"   — SEC DEF 14A: executive compensation, officer list
    "annual_report"     — SEC 10-K: officer/director list, brief bios
    "court_filing"      — CourtListener docket: parties, claims, outcome
    "form_990"          — IRS Form 990 XML sections: financials, board, officers

Usage:
    from osint.llm.extractors import DocumentExtractor
    from osint.llm.routing import LLMRouter

    extractor = DocumentExtractor(router)
    result = await extractor.extract(
        text=pdf_text,
        document_type="proxy_statement",
        entity_context={"name": "Acme Corp", "cik": "0001234567"},
    )
    # result["executives"] = [{name, title, base_salary, total_comp, year}, ...]

Notes:
    - All prompts return JSON. call_json() is used throughout.
    - Extraction is best-effort: missing fields are returned as None, not errors.
    - Text is truncated to protect context window. MAX_TEXT_CHARS controls this.
    - entity_context is injected into prompts to help disambiguate names
      (e.g., a proxy statement for "Acme" mentions people from "Acme" and other companies).
"""

from __future__ import annotations

import json
import logging
from typing import Any

from osint.llm.routing import LLMRouter, TaskType

log = logging.getLogger(__name__)

# Max characters of document text to pass to LLM.
# qwen3:14b has 16k context tokens ≈ ~48k characters.
# Leave room for the system prompt + entity context (~8k chars).
# → 36k chars of document text is the safe ceiling.
MAX_TEXT_CHARS = 36_000

# Supported document types and their corresponding TaskType constants.
_DOCUMENT_TYPE_MAP: dict[str, str] = {
    "proxy_statement": TaskType.DOCUMENT_EXTRACTION_PROXY,
    "annual_report":   TaskType.DOCUMENT_EXTRACTION_10K,
    "court_filing":    TaskType.DOCUMENT_EXTRACTION_COURT,
    "form_990":        TaskType.DOCUMENT_EXTRACTION_990,
}


# ─────────────────────────────────────────────────────────────────────────────
# System prompts — one per document type
# Each prompt specifies exactly what fields to return and in what format.
# Prompts are deliberately specific — vague prompts produce vague extractions.
# ─────────────────────────────────────────────────────────────────────────────

_SYSTEM_PROXY = """\
You are an expert at extracting executive compensation data from SEC DEF 14A proxy statements.

Extract the following fields from the provided document text and return a valid JSON object:

{
  "executives": [
    {
      "name": "Full name (string)",
      "title": "Exact title from document (string)",
      "base_salary": "Annual base salary in USD (integer, or null if not stated)",
      "bonus": "Annual bonus in USD (integer, or null)",
      "stock_awards": "Stock award value in USD (integer, or null)",
      "option_awards": "Option award value in USD (integer, or null)",
      "total_compensation": "Total compensation in USD (integer, or null)",
      "fiscal_year": "Fiscal year this compensation data covers (integer, or null)"
    }
  ],
  "directors": [
    {
      "name": "Full name (string)",
      "title": "Board title (string, e.g. 'Director', 'Lead Independent Director')",
      "committee_memberships": ["committee1", "committee2"]
    }
  ],
  "filing_company": "Company name this proxy is filed for (string)",
  "filing_year": "Year the proxy was filed (integer, or null)"
}

Rules:
- Only include named individuals explicitly listed in the compensation tables or director lists.
- Do not infer or estimate any dollar amounts — only extract amounts explicitly stated.
- If a field is not present in the document, return null for that field.
- Return ONLY the JSON object, no commentary, no markdown code blocks.
"""

_SYSTEM_10K = """\
You are an expert at extracting officer and director information from SEC 10-K annual reports.

Extract the following fields from the "Directors, Executive Officers and Corporate Governance"
section (or equivalent) and return a valid JSON object:

{
  "officers": [
    {
      "name": "Full name (string)",
      "age": "Age as integer (or null if not stated)",
      "title": "Exact title from document (string)",
      "bio_summary": "1-2 sentence summary of their background and experience (string)",
      "tenure_start_year": "Year they joined the company or took this role (integer, or null)"
    }
  ],
  "directors": [
    {
      "name": "Full name (string)",
      "age": "Age as integer (or null)",
      "title": "Board title (string)",
      "independence": "Independent or Non-Independent or null",
      "bio_summary": "1-2 sentence summary (string)"
    }
  ],
  "fiscal_year": "Fiscal year this 10-K covers (integer, or null)",
  "filing_company": "Company name (string)"
}

Rules:
- Focus exclusively on the named individuals section, not financial data.
- Summarize bios — do not reproduce verbatim text.
- Return ONLY the JSON object.
"""

_SYSTEM_COURT = """\
You are an expert at extracting structured information from federal and state court filings.

Extract the following fields from the provided court docket or filing text and return a valid JSON object:

{
  "case_name": "Official case name (string, e.g. 'Smith v. Acme Corp')",
  "case_number": "Court case number (string)",
  "court": "Name of the court (string, e.g. 'U.S. District Court, Eastern District of Pennsylvania')",
  "filing_date": "Date filed as YYYY-MM-DD (string, or null)",
  "case_type": "Type: 'civil', 'criminal', 'regulatory', 'bankruptcy', or 'other'",
  "plaintiffs": ["name1", "name2"],
  "defendants": ["name1", "name2"],
  "charges_or_claims": ["Brief description of each charge or claim (string)"],
  "outcome": "Outcome if known: 'pending', 'settled', 'dismissed', 'judgment_plaintiff', 'judgment_defendant', 'guilty', 'not_guilty', or null",
  "resolution_date": "Date resolved as YYYY-MM-DD (string, or null)",
  "monetary_judgment": "Dollar amount of any judgment or settlement in USD (integer, or null)",
  "summary": "1-3 sentence plain English summary of the case (string)"
}

Rules:
- Extract only information explicitly stated in the document.
- List ALL named parties on each side — do not truncate.
- Return ONLY the JSON object.
"""

_SYSTEM_990 = """\
You are an expert at extracting financial and governance information from IRS Form 990 XML data.

Extract the following fields from the provided Form 990 text or XML and return a valid JSON object:

{
  "organization_name": "Legal name of the organization (string)",
  "ein": "Employer Identification Number (string, or null)",
  "tax_year": "Tax year this 990 covers (integer, or null)",
  "total_revenue": "Total revenue in USD (integer, or null)",
  "total_expenses": "Total expenses in USD (integer, or null)",
  "total_assets": "Total assets in USD (integer, or null)",
  "net_assets": "Net assets / fund balance in USD (integer, or null)",
  "program_service_revenue": "Revenue from program services in USD (integer, or null)",
  "government_grants": "Government grants received in USD (integer, or null)",
  "total_grants_paid": "Total grants and similar amounts paid out in USD (integer, or null)",
  "officers": [
    {
      "name": "Full name (string)",
      "title": "Title (string)",
      "hours_per_week": "Reported hours per week (float, or null)",
      "compensation": "Reported compensation in USD (integer, or null)",
      "is_officer": "true/false",
      "is_key_employee": "true/false"
    }
  ],
  "board_members": [
    {
      "name": "Full name (string)",
      "title": "Board title (string)",
      "is_independent": "true or false or null"
    }
  ],
  "mission": "Organization's mission statement (1 sentence, or null)",
  "primary_program": "Description of primary program activity (1-2 sentences, or null)"
}

Rules:
- Return numeric fields as integers (remove commas, dollar signs).
- If a field is not present in the document, return null.
- Return ONLY the JSON object.
"""

_SYSTEM_PROMPTS: dict[str, str] = {
    TaskType.DOCUMENT_EXTRACTION_PROXY:  _SYSTEM_PROXY,
    TaskType.DOCUMENT_EXTRACTION_10K:    _SYSTEM_10K,
    TaskType.DOCUMENT_EXTRACTION_COURT:  _SYSTEM_COURT,
    TaskType.DOCUMENT_EXTRACTION_990:    _SYSTEM_990,
}


# ─────────────────────────────────────────────────────────────────────────────
# DocumentExtractor
# ─────────────────────────────────────────────────────────────────────────────

class DocumentExtractor:
    """
    LLM-powered structured extraction from unstructured documents.

    One instance per agent (or shared across agents — stateless).
    """

    def __init__(self, router: LLMRouter) -> None:
        self._router = router

    async def extract(
        self,
        text: str,
        document_type: str,
        entity_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Extract structured data from document text.

        Args:
            text:           Plain text of the document (from DocumentFetcher).
            document_type:  One of: "proxy_statement", "annual_report",
                            "court_filing", "form_990".
            entity_context: Optional dict with entity context to help LLM focus.
                            E.g., {"name": "Acme Corp", "city": "Philadelphia"}.
                            Injected as a preamble in the user prompt.

        Returns:
            Parsed dict with extracted fields. Structure depends on document_type.
            Empty dict if extraction fails or document_type is unsupported.
        """
        task_type = _DOCUMENT_TYPE_MAP.get(document_type)
        if task_type is None:
            log.warning(
                "document_extractor: unsupported document_type '%s'",
                document_type,
            )
            return {}

        system_prompt = _SYSTEM_PROMPTS.get(task_type, "")
        if not system_prompt:
            log.warning(
                "document_extractor: no system prompt for task_type '%s'",
                task_type,
            )
            return {}

        # Truncate text to stay within context window
        if len(text) > MAX_TEXT_CHARS:
            log.debug(
                "document_extractor: truncating text from %d to %d chars for '%s'",
                len(text), MAX_TEXT_CHARS, document_type,
            )
            text = text[:MAX_TEXT_CHARS]

        # Build user prompt
        user_prompt = self._build_user_prompt(text, document_type, entity_context)

        try:
            parsed, metadata = await self._router.call_json(
                task_type=task_type,
                prompt=user_prompt,
                system=system_prompt,
                temperature=0.05,  # Very low — extraction should be deterministic
            )
            log.debug(
                "document_extractor: extracted '%s' — %d tokens in, %d out",
                document_type,
                metadata.get("tokens_in", 0),
                metadata.get("tokens_out", 0),
            )
            return parsed or {}

        except Exception as exc:
            log.warning(
                "document_extractor: extraction failed for '%s': %s",
                document_type, exc,
            )
            return {}

    def _build_user_prompt(
        self,
        text: str,
        document_type: str,
        entity_context: dict[str, Any] | None,
    ) -> str:
        """Build the user prompt with optional entity context preamble."""
        parts: list[str] = []

        if entity_context:
            # Inject entity context so LLM can focus on the right entity
            ctx_lines = [f"  {k}: {v}" for k, v in entity_context.items() if v]
            if ctx_lines:
                parts.append(
                    "Entity context (use to focus your extraction on the correct entity):\n"
                    + "\n".join(ctx_lines)
                    + "\n"
                )

        parts.append(f"Document type: {document_type}\n")
        parts.append("Document text:\n---\n")
        parts.append(text)
        parts.append("\n---\n")
        parts.append("Extract the requested fields and return only the JSON object.")

        return "\n".join(parts)

    # ─────────────────────────────────────────────────────────────────────────
    # Typed convenience methods
    # ─────────────────────────────────────────────────────────────────────────

    async def extract_proxy(
        self,
        text: str,
        company_name: str,
        cik: str | None = None,
        filing_year: int | None = None,
    ) -> dict[str, Any]:
        """
        Extract executive compensation from a DEF 14A proxy statement.

        Returns:
            {
                "executives": [{name, title, base_salary, bonus, total_compensation, ...}],
                "directors": [{name, title, committee_memberships}],
                "filing_company": str,
                "filing_year": int | None,
            }
        """
        context: dict[str, Any] = {"name": company_name}
        if cik:
            context["cik"] = cik
        if filing_year:
            context["expected_year"] = str(filing_year)
        return await self.extract(text, "proxy_statement", context)

    async def extract_annual_report(
        self,
        text: str,
        company_name: str,
        fiscal_year: int | None = None,
    ) -> dict[str, Any]:
        """
        Extract officer/director list from a 10-K annual report.

        Returns:
            {
                "officers": [{name, age, title, bio_summary, tenure_start_year}],
                "directors": [{name, age, title, independence, bio_summary}],
                "fiscal_year": int | None,
                "filing_company": str,
            }
        """
        context: dict[str, Any] = {"name": company_name}
        if fiscal_year:
            context["fiscal_year"] = str(fiscal_year)
        return await self.extract(text, "annual_report", context)

    async def extract_court_filing(
        self,
        text: str,
        party_name: str,
    ) -> dict[str, Any]:
        """
        Extract case details from a court filing or docket.

        Returns:
            {
                "case_name": str,
                "case_number": str,
                "court": str,
                "filing_date": str | None,
                "case_type": str,
                "plaintiffs": [str],
                "defendants": [str],
                "charges_or_claims": [str],
                "outcome": str | None,
                "resolution_date": str | None,
                "monetary_judgment": int | None,
                "summary": str,
            }
        """
        context = {"party_name": party_name}
        return await self.extract(text, "court_filing", context)

    async def extract_form_990(
        self,
        text: str,
        organization_name: str,
        ein: str | None = None,
        tax_year: int | None = None,
    ) -> dict[str, Any]:
        """
        Extract financial and governance data from IRS Form 990.

        Returns:
            {
                "organization_name": str,
                "ein": str | None,
                "tax_year": int | None,
                "total_revenue": int | None,
                "total_expenses": int | None,
                "total_assets": int | None,
                "net_assets": int | None,
                "officers": [{name, title, compensation}],
                "board_members": [{name, title, is_independent}],
                "mission": str | None,
                "primary_program": str | None,
            }
        """
        context: dict[str, Any] = {"name": organization_name}
        if ein:
            context["ein"] = ein
        if tax_year:
            context["tax_year"] = str(tax_year)
        return await self.extract(text, "form_990", context)
