"""
AGENT 1: VACANCY ANALYZER (Рекрутер на першій лінії з динамічною пам'яттю фідбеку)

ЯКУ ЗАДАЧУ ВИРІШУЄ ЦЕЙ АГЕНТ:
1. Економія ресурсів: швидка фільтрація вакансій за стеком та рівнем досвіду (SKIP для невідповідних).
2. Структурування даних: перетворення сирого опису вакансії у суворий JSON-об'єкт (Pydantic Schema).
3. Адаптивне навчання (Agentic Memory): враховує історію реджектів користувача з Telegram.
4. Конфігурованість: читає профіль кандидата з candidate_profile.json (Zero Hardcode).
"""

import logging
from typing import List, Literal, Optional
from pydantic import BaseModel, Field
from google import genai
from google.genai import types

from config.config import settings

logger = logging.getLogger("ai_worker")


class AnalysisResult(BaseModel):
    match_score: int = Field(
        description="Fit score from 0 to 100 based on candidate profile and job requirements"
    )
    extracted_stack: List[str] = Field(
        description="List of primary technologies and tools required by the job (e.g. Go, PostgreSQL, Docker, gRPC)"
    )
    pros: List[str] = Field(
        description="Top 2-3 reasons why this vacancy fits the candidate"
    )
    missing_skills: List[str] = Field(
        description="Technologies or requirements demanded by the role that the candidate lacks"
    )
    decision: Literal["PROCEED", "SKIP"] = Field(
        description="'PROCEED' if match_score >= 60, otherwise 'SKIP'"
    )
    summary: str = Field(
        description="Brief 1-2 sentence summary of the evaluation"
    )


class VacancyAnalyzer:
    def __init__(self, api_key: str):
        self.client = genai.Client(api_key=api_key)
        self.model_name = "gemini-3.5-flash-lite"

        # Завантажуємо базовий профіль із candidate_profile.json
        self.candidate_profile = settings.profile_data.get(
            "baseline_prompt",
            "CANDIDATE BASELINE: Junior Go Backend Developer with PostgreSQL and RabbitMQ skills."
        )

    def analyze(self, title: str, description: str, recent_feedback: Optional[List[str]] = None) -> AnalysisResult:
        """
        Аналізує вакансію через Gemini зі Structured Outputs та динамічною пам'яттю реджектів.
        """
        logger.info(f"[Analyzer] Analyzing vacancy: '{title}' via {self.model_name}...")

        feedback_context = ""
        if recent_feedback:
            feedback_bullets = "\n".join([f"- {f}" for f in recent_feedback])
            feedback_context = f"""
        LEARNED USER PREFERENCES (Past Rejection Feedback from Telegram):
        The user has recently REJECTED job proposals with the following reasons:
        {feedback_bullets}
        IMPORTANT: If this vacancy triggers any of the user's past rejection patterns, lower the match_score and lean toward 'SKIP'.
        """

        system_instruction = f"""
        You are a strict technical recruiter and initial gatekeeper evaluating vacancies for a Go developer.
        Your job is to determine whether applying is worth the candidate's time.

        {self.candidate_profile}
        {feedback_context}

        SCORING RULES:
        1. If the job requires another language as primary (PHP, Java, C#, Senior C++) -> match_score < 20, decision: SKIP.
        2. If the job is Senior Go (3+ years experience required) -> match_score < 40, decision: SKIP.
        3. If the job is Junior / Junior+ / Middle Go with matching stack (Go + PostgreSQL/RabbitMQ/Docker) -> match_score >= 65, decision: PROCEED.
        4. Factor in the learned user preferences: respect user feedback history strictly.
        """

        prompt = f"""
        ANALYZE THIS VACANCY:
        Title: {title}
        Description:
        {description}
        """

        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=system_instruction,
                    response_mime_type="application/json",
                    response_schema=AnalysisResult,
                    temperature=0.2,
                ),
            )

            result: AnalysisResult = AnalysisResult.model_validate_json(response.text)
            logger.info(
                f"[Analyzer] Evaluated '{title}': Score={result.match_score}/100, Decision={result.decision}"
            )
            return result

        except Exception as err:
            logger.error(f"[Analyzer] Failed to analyze vacancy: {err}", exc_info=True)
            return AnalysisResult(
                match_score=0,
                extracted_stack=[],
                pros=[],
                missing_skills=["Analysis execution failed"],
                decision="SKIP",
                summary=f"Analysis failed due to error: {err}"
            )
