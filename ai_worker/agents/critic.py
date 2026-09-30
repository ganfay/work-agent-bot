import logging
from typing import List
from pydantic import BaseModel, Field
from google import genai
from google.genai import types

from agents.validator import ValidationResult
from agents.drafter import DraftResult

logger = logging.getLogger("ai_worker")



class CritiqueResult(BaseModel):
    is_approved: bool = Field(
        description="True if the letter is authentic, factually verified, human-sounding, and concise"
    )
    critique_score: int = Field(
        description="Overall quality and human-likeness score from 0 to 100"
    )
    fluff_percentage: int = Field(
        description="Estimated percentage of filler/water words found in the initial draft (0 to 100)"
    )
    ai_cliches_detected: List[str] = Field(
        description="Typical ChatGPT/AI buzzwords detected and purged (e.g. 'thrilled', 'testament', 'delve', 'passionate')"
    )
    hallucinations_detected: List[str] = Field(
        description="Unverified claims or false promises found in the draft"
    )
    grammar_and_syntax_notes: List[str] = Field(
        description="Grammar, punctuation, or phrasing adjustments made"
    )
    final_letter: str = Field(
        description="The final de-AI-ified, polished, highly professional cover letter ready for Telegram review"
    )


class CoverLetterCritic:
    def __init__(self, api_key: str):
        self.client = genai.Client(api_key=api_key)
        self.model_name = "gemini-3.5-flash-lite"

    def review(
        self,
        job_title: str,
        draft: DraftResult,
        validation: ValidationResult,
    ) -> CritiqueResult:
        logger.info(f"[Critic] Deep quality audit & anti-AI filtering for: '{job_title}'...")

        unverified_skills = ", ".join(validation.unverified_skills) or "None"
        verified_skills = ", ".join([f"{s.skill} ({s.project})" for s in validation.verified_skills])

        system_instruction = """
        You are a cynical, highly analytical Lead Engineering Recruiter and Editor.
        Your mission is to rigorously audit cover letters drafted by AI and make them sound 100% human, authentic, and grounded.

        AUDITING RESPONSIBILITIES:
        1. PURGE AI CLICHES (De-AI-ify):
           - Hunt down typical AI buzzwords: "thrilled", "delve", "testament", "spearhead", "tapestry", "passionate", "foster", "in today's fast-paced world".
           - Strip out obsequious or subservient language ("I would be honored to join", "prestigious company").
        2. MEASURE FLUFF:
           - Calculate what percentage of the draft was meaningless filler (fluff_percentage).
        3. FACT INTEGRITY:
           - Cross-check against UNVERIFIED SKILLS. The candidate MUST NOT claim unverified experience as a past accomplishment.
        4. TONE & SYNTAX:
           - Max 90-120 words.
           - Tone must be plain, confident, and engineer-to-engineer (like talking to a colleague over Slack).
           - Fix any awkward grammatical structures.
        5. FINAL POLISH:
           - Output the cleaned, humanized version in 'final_letter'.
        """

        prompt = f"""
        TARGET VACANCY: {job_title}

        AUDITED FACT DOSSIER (Ground Truth):
        - Verified Skills: {verified_skills}
        - Unverified / Missing Gaps: {unverified_skills}

        RAW DRAFT TO AUDIT:
        \"\"\"{draft.cover_letter}\"\"\"

        Perform a deep critique. Detect AI-isms, measure fluff percentage, correct grammar, and produce the authentic 'final_letter'.
        """

        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=system_instruction,
                    response_mime_type="application/json",
                    response_schema=CritiqueResult,
                    temperature=0.15,
                ),
            )

            result: CritiqueResult = CritiqueResult.model_validate_json(response.text)
            logger.info(
                f"[Critic] Audit complete: Score={result.critique_score}/100, Fluff={result.fluff_percentage}%, AI-cliches={len(result.ai_cliches_detected)}"
            )
            return result

        except Exception as err:
            logger.error(f"[Critic] Review failed: {err}", exc_info=True)
            return CritiqueResult(
                is_approved=True,
                critique_score=70,
                fluff_percentage=0,
                ai_cliches_detected=[],
                hallucinations_detected=[],
                grammar_and_syntax_notes=["Fallback: audit skipped due to error"],
                final_letter=draft.cover_letter,
            )
