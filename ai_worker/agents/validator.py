import logging
from typing import List
from pydantic import BaseModel, Field
from google import genai
from google.genai import types

logger = logging.getLogger("ai_worker")


class VerifiedSkill(BaseModel):
    skill: str = Field(
        description="Name of the technology or tool (e.g. RabbitMQ, PostgreSQL)"
    )
    project: str = Field(
        description="Project name where the candidate implemented this (e.g. SplitCore, Freelance Agent)"
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


class EvidenceValidator:
    def __init__(self, api_key: str):
        self.client = genai.Client(api_key=api_key)
        self.model_name = "gemini-3.5-flash-lite"

        # У майбутньому це заміниться на прямі векторні запити до pgvector
        self.codebase_dossier = """
        ================================================================================
CANDIDATE CODEBASE EVIDENCE DOSSIER: MAKSYM BIESIEDIN (ganfay)
================================================================================

1. PROJECT: SplitCore (Production Fintech Platform & Microservices Ecosystem)
   • Repository: github.com/ganfay/split-core
   • Scale & Status: Production Monorepo (>105 commits, v1.2.0)
   • Live Endpoints: Dashboard: thesplitcore.tech | API: ganfay.me

   [Architecture & System Design]
   • Clean Architecture / DDD: Strict separation across Domain, Use Case,
     Repository, and Delivery layers. Interfaces decoupled via inversion of control.
   • Multi-Service Topology:
     - `split-core (web)`: RESTful HTTP API & Swagger documentation engine.
     - `split-core (bot)`: Telegram stateful bot engine & internal gRPC server.
     - `split-notify`: Dedicated asynchronous worker consuming RabbitMQ & communicating
       with core via gRPC client.
     - `frontend`: Single-page application built on React 19, TypeScript, Vite, and Tailwind CSS.
     - `proto`: Centralized Protocol Buffers v3 contract repository (github.com/ganfay/split-proto).

   [Algorithms & Domain Logic]
   • Greedy Debt Simplification Algorithm (Graph Settlement):
     - Proprietary settlement engine (`calculateSettlements` in internal/usecase/fund_usecase.go).
     - Reduces an $O(N^2)$ bipartite debt network to a minimal $O(N)$ transaction set.
     - Segregates net balances into discrete Creditor and Debtor queues; iteratively matches
       and clears pairwise obligations using greedy reduction with 2-decimal precision rounding.
     - Fully verified with Table-Driven Unit Tests using `testify/assert` covering edge cases,
       equal splits, and floating-point precision bounds.

   [Inter-Service Communication & Networking]
   • gRPC & Protocol Buffers v3:
     - Internal synchronous RPC communication via `google.golang.org/grpc` (v1.82.0) and `protobuf` (v1.36.11).
     - Defined `NotificationService` exposing `GetNotificationTargets`, `NotificateTarget`, and `SayPing`.
     - Internal microservice routing isolating bot dispatch logic from notification generation.
   • HTTP Delivery Layer:
     - High-performance Go standard library routing (`net/http` ServeMux with Go 1.22+ pattern matching).
     - Granular CORS middleware and centralized panic-recovery wrappers.
     - Auto-generated OpenAPI / Swagger specs via `swaggo/swag` exposed at `/api/v1/swagger/`.

   [State Management & Authentication]
   • Redis Finite State Machine (FSM) & Lazy Loading:
     - Stateful Telegram dialog engine (`domain.UserContext`) with 15+ discrete states 
       persisted in Redis (`go-redis/v9`) with TTL.
     - Fault-tolerant Lazy Loading: handles cache expirations / `redis.Nil` misses by
       seamlessly falling back to PostgreSQL (`GetOrCreateRealUser`), lazily reconstructing
       the session context and persisting it back via deferred closures.
   • Passwordless Telegram Deep-Link Web Authentication:
     - Hybrid cross-platform auth workflow: frontend requests session (`POST /api/v1/auth/telegram/init`),
       generating an ephemeral UUID session in Redis (3-min TTL).
     - Deep-link redirection to Telegram bot (`/start auth_<uuid>`); bot verifies session, binds
       internal DB user ID, and marks session as confirmed.
     - Token exchange issuance: Dual JWTs (Access Token 15m, Refresh Token 7d) signed with
       HMAC-SHA256 (`golang-jwt/jwt/v5`).

   [Database Engineering (PostgreSQL)]
   • Driver: Pure raw SQL execution using `jackc/pgx/v5` and `pgxpool` (zero ORM overhead).
   • Concurrency & Pooling: Custom connection pool tuning, health checks, and context timeouts.
   • Transaction Management: Explicit ACID transactions (`tx.Begin`, `tx.Commit`) with safe
     deferred rollbacks checking `pgx.ErrTxClosed` for atomic multi-table writes.
   • Data Integrity: Surrogate keys (`SERIAL PRIMARY KEY`), foreign key constraints with 
     `ON DELETE CASCADE`, composite keys (`fund_members`), and strict NULL-handling via `COALESCE`.
   • Defensive SQL: Idempotent user synchronization via `ON CONFLICT (tg_id) DO UPDATE` and `DO NOTHING`.
   • Migrations: Versioned, reversible SQL migrations via `golang-migrate` executed through 
     isolated Docker containers.

   [Message Broker & Event-Driven Architecture]
   • RabbitMQ via `amqp091-go`:
     - Asynchronous event publication (`expense_created`) decoupled from synchronous user requests.
     - Raft-based Quorum Queues (`amqp.QueueTypeQuorum`) configured for high availability
       and persistent, durable message delivery.
     - Clean separation between event producer (core backend) and consumer (notifier worker).

   [Telegram Bot Engineering]
   • Framework: `gopkg.in/telebot.v4` running Long Polling mode.
   • Crash Resilience: Strict HTML parse mode (`tele.ModeHTML`) preventing MarkdownV2 entity-escaping crashes.
   • Sanitization: Global XSS protection and input sanitization (`html.EscapeString`).
   • Observability: Custom `telebot` logging middleware measuring latency per handler.

   [DevOps, CI/CD & Production Infrastructure]
   • CI/CD Pipelines (GitHub Actions):
     - `ci.yml`: Automated pull request & push validation, linting backend and notifier via 
       `golangci-lint-action`, and executing automated test suites (`go test -v ./...`).
     - `deploy.yml`: Multi-stage Docker image builds for 4 targets (`web`, `bot`, `notify`, `frontend`),
       automated tagging and pushing to GitHub Container Registry (`ghcr.io`), zero-downtime SSH 
       remote deployment on Azure VPS, automated migration execution, and image pruning.
   • Containerization: Multi-stage Dockerfiles (`CGO_ENABLED=0`, minimal Alpine runtime).
   • Production Reverse Proxy: `Caddy 2.10` with automated Let's Encrypt SSL/TLS certificates.
   • Network Hardening: Zero exposed internal ports. Only ports 80 and 443 are reachable externally;
     PostgreSQL, Redis, RabbitMQ, and gRPC run exclusively within an internal Docker bridge network (`split_prod_network`).
   • Host Hardening: Microsoft Azure VPS (Ubuntu 24.04 LTS), non-root execution (`ganfay`),
     disabled password SSH authentication, UFW firewall enforcement, and `unattended-upgrades`.

   [Observability & Reliability]
   • Structured Logging: Standard library `log/slog` with environment-based routing (JSON in prod, Debug text in local).
   • Log Rotation: File-level rotation via `lumberjack.v2` (10 MB size, 5 backups, 30 days retention, gzip compression)
     using `io.MultiWriter` for simultaneous disk and stdout streams.
   • Graceful Shutdown: OS signal trapping (`SIGINT`, `SIGTERM`) cleanly draining HTTP listeners,
     Telegram polling loops, gRPC servers, AMQP channels, Redis clients, and PostgreSQL pools.

--------------------------------------------------------------------------------

2. PROJECT: Freelance Agent (Polyglot Microservices Pipeline)
   • Scraper (Go): High-performance RSS parsing via `gofeed`, atomic deduplication in PostgreSQL
     via `ON CONFLICT DO NOTHING`, structured logging (`slog` + `lumberjack`), automated migrations
     via `golang-migrate`, graceful shutdown via `signal.NotifyContext`.
   • AI Worker (Python): RabbitMQ consumer via `pika` with prefetch QoS, Google Gemini API
     integration leveraging strict Structured Outputs (Pydantic models).
   • Broker: RabbitMQ running in Docker with dedicated exchange and queue topology.

================================================================================
CONFIRMED GAPS & LIMITATIONS (HONEST TECHNICAL BOUNDARIES):
================================================================================
• Apache Kafka: NOT confirmed (RabbitMQ with Quorum queues is confirmed).
• Kubernetes / Helm: NOT confirmed (Docker Compose multi-container production deployment is confirmed).
• AWS / GCP Native Infrastructure: NOT confirmed (Microsoft Azure VPS, DigitalOcean, and Linux VPS hardening are confirmed).
• Distributed Tracing: OpenTelemetry / Jaeger NOT yet integrated (relies on structured logging, request duration middleware, and lumberjack rotation).
        """

    def validate(self, extracted_stack: List[str]) -> ValidationResult:
        logger.info(f"[Validator] Cross-referencing {len(extracted_stack)} technologies against codebase dossier...")

        system_instruction = f"""
        You are a meticulous technical auditor and fact-checker.
        Your mission is to examine a list of technologies extracted from a job posting and verify which ones the candidate has ACTUALLY implemented based STRICTLY on the codebase dossier.

        {self.codebase_dossier}

        STRICT AUDITING RULES:
        1. Only approve a technology in 'verified_skills' if there is clear, concrete evidence in the dossier.
        2. If a technology is NOT mentioned in the dossier or is listed under CONFIRMED GAPS (e.g. gRPC, Kafka, Kubernetes), it MUST be placed in 'unverified_skills'.
        3. Do NOT make assumptions or hallucinate implementations that are not documented in the dossier.
        4. Provide concise, high-density engineering evidence for each verified skill.
        """

        prompt = f"""
        VERIFY THESE EXTRACTED TECHNOLOGIES:
        {extracted_stack}
        """

        try:
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

            result: ValidationResult = ValidationResult.model_validate_json(response.text)

            logger.info(
                f"[Validator] Verification complete: {len(result.verified_skills)} verified, {len(result.unverified_skills)} gaps."
            )
            return result

        except Exception as err:
            logger.error(f"[Validator] Failed to validate evidence: {err}", exc_info=True)
            return ValidationResult(
                verified_skills=[],
                unverified_skills=extracted_stack,
                dossier_summary=f"Validation failed due to error: {err}"
            )
