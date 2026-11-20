"""
AGENT 2: EVIDENCE VALIDATOR (Векторний детектив фактів на базі pgvector)

ЯКУ ЗАДАЧУ ВИРІШУЄ ЦЕЙ АГЕНТ:
1. Замінює захардкоджені описи на повноцінний RAG (Retrieval-Augmented Generation).
2. Робить семантичний векторний пошук у PostgreSQL (через pgvector) для кожної технології з вакансії.
3. Якщо схожість векторів низька (наприклад, для Kubernetes чи gRPC) — фіксує це як прогалину (unverified_skills).
"""

import logging
from typing import List, Optional
from pydantic import BaseModel, Field
from google import genai
from google.genai import types

from config.config import settings
from core.llm import call_with_retry
from rag.retriever import retriever

logger = logging.getLogger("ai_worker")


# ============================================================================
# 1. СХЕМА ДАНИХ (СТРУКТУРА) ДЛЯ ВІДПОВІДІ ШІ
# ============================================================================
class VerifiedSkill(BaseModel):
    skill: str = Field(
        description="Name of the technology or tool (e.g. RabbitMQ, PostgreSQL)"
    )
    project: str = Field(
        description="Project name retrieved from pgvector (e.g. SplitCore, Freelance Agent)"
    )
    evidence: str = Field(
        description="Specific architectural implementation detail proving practical experience"
    )


class ValidationResult(BaseModel):
    verified_skills: List[VerifiedSkill] = Field(
        description="List of requested skills that are strictly backed by proof in the candidate's codebase"
    )
    unverified_skills: List[str] = Field(
        description="List of requested skills that the candidate has NO verified implementation for"
    )
    dossier_summary: str = Field(
        description="Objective 1-2 sentence technical summary of candidate's verified readiness"
    )


# ============================================================================
# 2. КЛАС АГЕНТА-ВАЛІДАТОРА (RAG + pgvector)
# ============================================================================
class EvidenceValidator:
    def __init__(self, api_key: str, model_name: Optional[str] = None):
        self.client = genai.Client(api_key=api_key)
        self.model_name = model_name or settings.gemini_model

    def validate(self, extracted_stack: List[str]) -> ValidationResult:
        """
        Для кожної технології робить векторний пошук у pgvector,
        збирає знайдені шматки коду та передає в Gemini для строгого аудиту.
        """
        if not extracted_stack:
            logger.info("[Validator:pgvector] No technologies extracted; skipping RAG lookup and LLM call.")
            return ValidationResult(
                verified_skills=[],
                unverified_skills=[],
                dossier_summary="No specific technical stack requirements were extracted from the vacancy.",
            )

        logger.info(f"[Validator:pgvector] Querying vector database for {len(extracted_stack)} technologies...")

        # 1. ВЕКТОРНИЙ RAG: шукаємо докази в базі pgvector за одне підключення до PostgreSQL
        batch_matches = retriever.search_evidence_batch(queries=extracted_stack, limit=1, threshold=0.48)
        retrieved_context_blocks = []

        for tech in extracted_stack:
            matches = batch_matches.get(tech, [])
            if matches:
                top = matches[0]
                retrieved_context_blocks.append(
                    f"Technology: {tech} -> MATCHED [{top['project']} / {top['category']}] (Similarity: {top['similarity']}): {top['content']}"
                )
            else:
                retrieved_context_blocks.append(
                    f"Technology: {tech} -> NO RELEVANT PROOF FOUND in pgvector codebase"
                )

        dynamic_dossier = "\n".join(retrieved_context_blocks)
        logger.info(f"[Validator:pgvector] Dynamic RAG context assembled ({len(retrieved_context_blocks)} entries).")

        # 2. ПРОМПТ ДЛЯ АУДИТУ ЗІБРАНИХ ВЕКТОРНИХ ДОКАЗІВ
        system_instruction = """
        You are a meticulous technical auditor and fact-checker.
        Your mission is to examine a list of technologies and verify which ones the candidate has ACTUALLY implemented based STRICTLY on the pgvector evidence dossier provided.

        STRICT AUDITING RULES:
        1. Only approve a technology in 'verified_skills' if there is clear MATCHED proof from pgvector.
        2. If an entry says "NO RELEVANT PROOF FOUND in pgvector codebase", you MUST place it in 'unverified_skills'.
        3. Do NOT hallucinate implementations that are not in the dynamic dossier.
        """

        prompt = f"""
        DYNAMIC PGVECTOR DOSSIER (Retrieved from PostgreSQL via Cosine Similarity):
        {dynamic_dossier}

        TARGET TECHNOLOGIES TO VERIFY:
        {extracted_stack}

        Classify into verified_skills and unverified_skills strictly based on the pgvector search results.
        """

        def _call():
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=system_instruction,
                    response_mime_type="application/json",
                    response_schema=ValidationResult,
                    temperature=0.1,
                ),
            )
            if not response.text:
                raise RuntimeError("Gemini Validator returned empty response text")
            return ValidationResult.model_validate_json(response.text)

        result: ValidationResult = call_with_retry(_call, operation="EvidenceValidator.validate")
        logger.info(
            f"[Validator:pgvector] Verification complete: {len(result.verified_skills)} verified, {len(result.unverified_skills)} gaps."
        )
        return result
