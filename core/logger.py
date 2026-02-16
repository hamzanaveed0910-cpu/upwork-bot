import logging
import sys
import io
from datetime import datetime
from logging.handlers import RotatingFileHandler


# This class controls how logs look in the terminal
# It adds colors and a clean format to messages
class CustomFormatter(logging.Formatter):

    # These are color codes for the terminal
    # They make logs easier to read
    grey = "\x1b[38;20m"
    green = "\x1b[32;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    # This is the log message structure
    # Example:
    # [19:45:22] [INFO] Fetching jobs...
    format = "[%(asctime)s] [%(levelname)s] %(message)s"

    # Decide which color to use for which log type
    FORMATS = {
        logging.DEBUG: grey + format + reset,      # Debug → grey
        logging.INFO: green + format + reset,      # Info → green
        logging.WARNING: yellow + format + reset, # Warning → yellow
        logging.ERROR: red + format + reset,       # Error → red
        logging.CRITICAL: bold_red + format + reset # Critical → bold red
    }
    
    # This function is called every time we log something
    def format(self, record):
        # Pick the correct color based on log level
        log_fmt = self.FORMATS.get(record.levelno)

        # Create a formatter with time in HH:MM:SS format
        formatter = logging.Formatter(log_fmt, datefmt="%H:%M:%S")

        # Return the final formatted message
        return formatter.format(record)


# Create a logger for the Upwork bot
log = logging.getLogger("UpworkBot")

# Set minimum log level (INFO and above will show)
log.setLevel(logging.INFO)

# This sends logs to the terminal (console)
try:
    # Prefer to reconfigure sys.stdout to UTF-8 where supported (Python 3.7+)
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        stream = sys.stdout
    except Exception:
        # Fallback: wrap the binary buffer with a TextIOWrapper that replaces
        # characters that can't be encoded to avoid crashing the logger.
        stream = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

    ch = logging.StreamHandler(stream)
    ch.setFormatter(CustomFormatter())
    log.addHandler(ch)
except Exception:
    # Last-resort: create a basic StreamHandler (will use default encoding)
    ch = logging.StreamHandler()
    ch.setFormatter(CustomFormatter())
    log.addHandler(ch)

# Also log to a rotating file for production diagnostics (UTF-8)
try:
    fh = RotatingFileHandler('bot.log', maxBytes=5 * 1024 * 1024, backupCount=5, encoding='utf-8')
    fh.setFormatter(CustomFormatter())
    log.addHandler(fh)
except Exception:
    # fallback to basic FileHandler
    fh = logging.FileHandler('bot.log', encoding='utf-8')
    fh.setFormatter(CustomFormatter())
    log.addHandler(fh)
