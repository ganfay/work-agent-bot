import sys
from broker.consumer import RabbitMQConsumer
from config.config import settings
from core.logger import setup_logger
from agents.analyzer import VacancyAnalyzer
from agents.validator import EvidenceValidator
from agents.drafter import CoverLetterDrafter
from agents.critic import CoverLetterCritic

# 1. Initialize logger
logger = setup_logger(settings.log_path)

# 2. Initialize our 4 agents
if not settings.gemini_api_key:
    logger.critical("GEMINI_API_KEY is not set in .env! Exiting.")
    sys.exit(1)

analyzer = VacancyAnalyzer(api_key=settings.gemini_api_key)
validator = EvidenceValidator(api_key=settings.gemini_api_key)
drafter = CoverLetterDrafter(api_key=settings.gemini_api_key)
critic = CoverLetterCritic(api_key=settings.gemini_api_key)


# 3. Vacancy processing handler
def process_vacancy(job: dict, publisher_func):
    title = job.get("Title", "No Title")
    link = job.get("Link", "")
    description = job.get("Description", "")

    logger.info("==================================================")
    logger.info(f"Incoming job from queue: '{title}'")
    logger.info(f"Link: {link}")

    # ----------------------------------------------------
    # AGENT 1: ANALYZER (Fast filtering and scoring)
    # ----------------------------------------------------
    analysis = analyzer.analyze(title=title, description=description)
    logger.info(f"--> [1. ANALYZER] Score: {analysis.match_score}/100 | Decision: {analysis.decision}")

    if analysis.decision == "SKIP":
        logger.info(f"[-] Vacancy '{title}' skipped (score too low). Pipeline finished.")
        return

    # ----------------------------------------------------
    # AGENT 2: EVIDENCE VALIDATOR (Fact-checking against codebase)
    # ----------------------------------------------------
    validation = validator.validate(extracted_stack=analysis.extracted_stack)
    logger.info(f"--> [2. VALIDATOR] Verified: {len(validation.verified_skills)} | Gaps: {len(validation.unverified_skills)}")

    # ----------------------------------------------------
    # AGENT 3: COVER LETTER DRAFTER (Initial draft generation)
    # ----------------------------------------------------
    draft = drafter.draft(title=title, description=description, validation=validation)
    logger.info(f"--> [3. DRAFTER] Draft generated ({draft.word_count} words).")

    # ----------------------------------------------------
    # AGENT 4: CRITIC & QUALITY ASSURANCE (De-AI-ify and Polish)
    # ----------------------------------------------------
    critique = critic.review(job_title=title, draft=draft, validation=validation)
    logger.info(f"--> [4. CRITIC] Overall Score: {critique.critique_score}/100 | Fluff: {critique.fluff_percentage}%")

    logger.info("==================================================")
    logger.info("FINAL PRODUCTION-READY PROPOSAL:")
    logger.info("--------------------------------------------------")
    logger.info(f"\n{critique.final_letter}\n")
    logger.info("--------------------------------------------------")

    # ----------------------------------------------------
    # PUBLISH TO REVIEW QUEUE (For Go Telegram Bot HITL)
    # ----------------------------------------------------
    review_payload = {
        "title": title,
        "link": link,
        "match_score": analysis.match_score,
        "summary": analysis.summary,
        "pros": analysis.pros,
        "fluff_percentage": critique.fluff_percentage,
        "final_letter": critique.final_letter,
    }

    publisher_func(review_payload)
    logger.info(f"[+] Vacancy '{title}' forwarded to review_queue for Telegram Bot!")


def main():
    logger.info("Starting AI Worker Service with full 4-Agent Pipeline...")

    consumer = RabbitMQConsumer(
        amqp_url=settings.rabbitmq_url(),
        queue_name=settings.queue_name
    )

    try:
        consumer.connect()
        consumer.start_consuming(message_handler=process_vacancy)

    except KeyboardInterrupt:
        logger.info("Shutdown signal received (Ctrl+C). Closing worker...")
        consumer.close()
        sys.exit(0)

    except Exception as err:
        logger.critical(f"Fatal worker error: {err}", exc_info=True)
        consumer.close()
        sys.exit(1)


if __name__ == "__main__":
    main()
