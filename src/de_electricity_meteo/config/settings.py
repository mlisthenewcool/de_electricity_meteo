"""Application settings and constants.

This module centralizes all configurable parameters for the application,
including logging, download and retry settings.
"""

# =============================================================================
# Logging Settings
# =============================================================================

# Minimum log level: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_LEVEL: str = "DEBUG"

# =============================================================================
# Download Settings
# =============================================================================

# Chunk size for streaming downloads (in bytes)
DOWNLOAD_CHUNK_SIZE: int = 1024 * 1024  # 1 MB

# Timeout settings (in seconds)
DOWNLOAD_TIMEOUT_TOTAL: int = 600  # 10 minutes - max time for entire download
DOWNLOAD_TIMEOUT_CONNECT: int = 10  # 10 seconds - max time to establish connection
DOWNLOAD_TIMEOUT_SOCK_READ: int = 30  # 30 seconds - max time between data packets

# =============================================================================
# Retry Settings
# =============================================================================

# Maximum number of retry attempts for failed operations
RETRY_MAX_ATTEMPTS: int = 3

# Initial delay between retries (in seconds)
RETRY_INITIAL_DELAY: float = 1.0

# Multiplier applied to delay after each failed attempt
RETRY_BACKOFF_FACTOR: float = 2.0
