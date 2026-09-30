import logging
from typing import List
from pydantic import BaseModel, Field
from google import genai
from google.genai import types

from agents.validator import ValidationResult

logger = logging.getLogger("ai_worker")


class DraftResult(BaseModel):
    cover_letter: str = Field(
        description="The complete, high-impact cover letter ready to be sent to recruiters (max 100-130 words)"
    )
    highlighted_proofs: List[str] = Field(
        description="Specific project implementation facts included in the text"
    )
    word_count: int = Field(
        description="Total word count of the cover letter"
    )


class CoverLetterDrafter:
    def __init__(self, api_key: str):
        self.client = genai.Client(api_key=api_key)
        self.model_name = "gemini-3.5-flash-lite"

    def draft(self, title: str, description: str, validation: ValidationResult) -> DraftResult:
        logger.info(f"[Drafter] Drafting personalized cover letter for: '{title}'...")

        verified_points = "\n".join([
            f"- {item.skill} ({item.project}): {item.evidence}"
            for item in validation.verified_skills
        ])

        unverified_points = ", ".join(validation.unverified_skills) or "None"

        system_instruction = """
        You are an elite tech copywriter writing tailored, high-conversion job proposals for a Go Backend Developer.

        STRICT WRITING RULES:
        1. LENGTH: Maximum 100-130 words. Zero fluff, zero corporate clichés ("I am eager", "hardworking", "passionate").
        2. TONE: Confident, direct, engineering-oriented, peer-to-peer (developer to developer).
        3. EVIDENCE: Mention 1-2 concrete technical proofs from the VERIFIED SKILLS list (e.g. SplitCore, RabbitMQ, pgxpool).
        4. GAPS: If UNVERIFIED SKILLS are critical to the job, address them honestly in one short sentence without apologizing (e.g., "While my message broker experience is currently centered on RabbitMQ, I can pick up Kafka quickly").
        5. CALL TO ACTION: A simple, natural closing (e.g., "Looking forward to hearing your thoughts or discussing the architecture").
        """

        prompt = f"""
        JOB VACANCY:
        Title: {title}
        Description:
        {description[:1500]}

        AUDITED CANDIDATE EVIDENCE:
        Verified Skills with Project Proofs:
        {verified_points}

        Unverified / Missing Gaps:
        {unverified_points}

        Draft a concise, tailored cover letter based STRICTLY on the verified facts above.
        """

        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=system_instruction,
                    response_mime_type="application/json",
                    response_schema=DraftResult,
                    temperature=0.5,
                ),
            )

            result: DraftResult = DraftResult.model_validate_json(response.text)
            logger.info(f"[Drafter] Generated letter ({result.word_count} words).")
            return result

        except Exception as err:
            logger.error(f"[Drafter] Failed to draft cover letter: {err}", exc_info=True)
            return DraftResult(
                cover_letter="Failed to generate draft due to an internal error.",
                highlighted_proofs=[],
                word_count=0
            )
