import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    SCOREPLEX_API_KEY = os.getenv("SCOREPLEX_API_KEY")
    SCOREPLEX_BASE_URL = os.getenv("SCOREPLEX_BASE_URL", "https://api.scoreplex.io/api/v1")
    
    # Batch/concurrency: 1 = sequential (like Flask), avoids rate limits; keep batching logic for flexibility
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1"))

    # Polling settings (match Flask: longer timeout so data_leak can complete)
    MAX_POLL_ATTEMPTS = int(os.getenv("MAX_POLL_ATTEMPTS", "60"))
    POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "2"))  # Check every 2 seconds
    POLL_TIMEOUT_SECONDS = int(os.getenv("POLL_TIMEOUT_SECONDS", "300"))  # Max seconds per row (Flask default 300 = 5 min)
    
    # Required statuses for early exit
    REQUIRED_STATUSES = ["email_status", "phone_status", "data_leak_status"]
    COMPLETE_VALUES = ["COMPLETE", "COMPLETED", "SUCCESS"]

settings = Settings()
