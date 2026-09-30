import logging
from typing import List, Literal
from pydantic import BaseModel, Field
from google import genai
from google.genai import types

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

        self.candidate_profile = """
        CANDIDATE BASELINE:
        - Target Role: Junior / Junior+ Go Backend Developer (~1 year experience, SplitCore pet project).
        - Core Tech Stack: Go (Golang), PostgreSQL, pgxpool, RabbitMQ, Docker, Clean Architecture, REST API, Linux.
        - Additional: Learning Python (AI agents, basic RAG).
        - Gaps / No Commercial Experience: Kubernetes (K8s), Apache Kafka, gRPC, high-scale microservices, AWS cloud architecture, complex CI/CD.
        - HARD REJECT CRITERIA (Instant SKIP): Senior positions (3+ years required), non-Go primary languages (Java, PHP, C#, C++, Node.js), on-site outside Kyiv.
        """

    def analyze(self, title: str, description: str) -> AnalysisResult:
        logger.info(f"[Analyzer] Analyzing vacancy: '{title}' via {self.model_name}...")

        system_instruction = f"""
        You are a strict technical recruiter and initial gatekeeper evaluating vacancies for a Go developer.
        Your job is to determine whether applying is worth the candidate's time.

        {self.candidate_profile}

        SCORING RULES:
        1. If the job requires another language as primary (PHP, Java, C#, Senior C++) -> match_score < 20, decision: SKIP.
        2. If the job is Senior Go (3+ years experience required) -> match_score < 40, decision: SKIP.
        3. If the job is Junior / Junior+ / Middle Go with matching stack (Go + PostgreSQL/RabbitMQ/Docker) -> match_score >= 65, decision: PROCEED.
        4. Be objective and strict. Do not inflate scores artificially.
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
