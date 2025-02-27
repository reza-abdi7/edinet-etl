import logging
from datetime import datetime
from pathlib import Path

from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOG_DIR = PROJECT_ROOT / 'log'
LOG_DIR.mkdir(parents=True, exist_ok=True)  # Ensure log directory exists

logger = logging.getLogger('edinet')
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    '[%(asctime)s] %(levelname)s: %(message)s', '%Y-%m-%d %H:%M:%S'
)

log_file = LOG_DIR / f"edinet_{datetime.now().strftime('%Y%m%d')}.log"

file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)
logger.addHandler(file_handler)


# Custom logging handlers that work with tqdm
class TqdmLoggingHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            # Use tqdm.write which is compatible with progress bars
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)


# Console handler using tqdm-compatible handler
console_handler = TqdmLoggingHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(
    logging.WARNING
)  # Only WARNING and above will be shown on console.
logger.addHandler(console_handler)

# Remove other handlers to avoid duplication
for handler in logger.handlers[:]:
    if not isinstance(handler, (logging.FileHandler, TqdmLoggingHandler)):
        logger.removeHandler(handler)
