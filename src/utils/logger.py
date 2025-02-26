import logging
import os
from datetime import datetime
from tqdm import tqdm
from config.config import config

# Create a logger for the application
logger = logging.getLogger('edinet')
logger.setLevel(logging.INFO)  # Changed back to INFO as default level

# Formatter for logging messages - simplified but informative
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s', '%Y-%m-%d %H:%M:%S')

# Create log directory in root if it doesn't exist
log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'log')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Regular log file
current_date = datetime.now().strftime('%Y%m%d')
log_file = os.path.join(log_dir, f'edinet_{current_date}.log')
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
console_handler.setLevel(logging.WARNING)  # Only WARNING and above will be shown on console.
logger.addHandler(console_handler)

# Remove other handlers to avoid duplication
for handler in logger.handlers[:]:
    if not isinstance(handler, (logging.FileHandler, TqdmLoggingHandler)):
        logger.removeHandler(handler)
