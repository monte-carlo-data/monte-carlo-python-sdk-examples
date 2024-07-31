import datetime
import logging
import os
import time
from rich import print
from pathlib import Path

LOGGER = logging.getLogger()
LOGS_DIR = Path(str(Path(os.path.abspath(__file__)).parent.parent.parent) + "/logs")


class LoggingConfigs(object):

    @staticmethod
    def logging_configs(util_name) -> dict:
        """Return the Python Logging Configuration Dictionary.

        Returns:
            dict: Python Logging Configurations.

        """

        LOGS_DIR.mkdir(parents=True, exist_ok=True)

        logging_config = dict(
            version=1,
            formatters={
                'standard': {'format': '%(asctime)s - %(levelname)s - %(message)s'},
                'console': {'format': '%(levelname)s - %(message)s'}
            },
            handlers={
                'file': {'class': 'logging.FileHandler',
                         'formatter': 'standard',
                         'level': logging.DEBUG,
                         'filename': f"{LOGS_DIR}/{util_name}-{datetime.date.today()}.log",
                         'encoding': "utf-8"},
                'console': {'class': 'logging.StreamHandler',
                            'formatter': 'console',
                            'level': logging.INFO,
                            'stream': 'ext://sys.stdout'}
            },
            root={'handlers': ['file', 'console'],
                  'level': logging.NOTSET},
        )

        return logging_config


class LogHelper(object):
    """Formatted Log Messages"""

    @staticmethod
    def banner():
        font = f"""
                    
            ███╗   ███╗ ██████╗ ███╗   ██╗████████╗███████╗     ██████╗ █████╗ ██████╗ ██╗      ██████╗ 
            ████╗ ████║██╔═══██╗████╗  ██║╚══██╔══╝██╔════╝    ██╔════╝██╔══██╗██╔══██╗██║     ██╔═══██╗
            ██╔████╔██║██║   ██║██╔██╗ ██║   ██║   █████╗      ██║     ███████║██████╔╝██║     ██║   ██║
            ██║╚██╔╝██║██║   ██║██║╚██╗██║   ██║   ██╔══╝      ██║     ██╔══██║██╔══██╗██║     ██║   ██║
            ██║ ╚═╝ ██║╚██████╔╝██║ ╚████║   ██║   ███████╗    ╚██████╗██║  ██║██║  ██║███████╗╚██████╔╝
            ╚═╝     ╚═╝ ╚═════╝ ╚═╝  ╚═══╝   ╚═╝   ╚══════╝     ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝ ╚═════╝ 
                                                                                                         
        """
        print(f"[dodger_blue2]{font}")

    @staticmethod
    def split_message(message: str, level: [logging.ERROR, logging.INFO] = logging.INFO):
        """Writes message from stderr/stdout to individual lines.

            Args:
                message(str): Output from stdout or stderr.
                level(LOGGER): Logging level in which the lines are printed.

        """
        for line in message.split('\n'):
            if line != '':
                LOGGER.log(level, line)


class LogRotater(object):
    """Rotate Logs Every N Days."""

    @staticmethod
    def rotate_logs(retention_period: int):
        """Delete log files older than the retention period.

        Args:
            retention_period (int): Number of Days of Logs to retain.

        """
        now = time.time()

        for log_file in os.listdir(LOGS_DIR):
            log = os.path.join(LOGS_DIR, log_file)
            if os.stat(log).st_mtime < now - retention_period * 86400:
                if os.path.isfile(log):
                    os.remove(log)
