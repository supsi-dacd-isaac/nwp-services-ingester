from logging.handlers import TimedRotatingFileHandler
import zipfile
import os


class ZippedTimedRotatingFileHandler(TimedRotatingFileHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def doRollover(self):
        # Rotate the log file first
        super().doRollover()

        # Zip the previous log file
        prev_log = self.baseFilename + ".1"
        if os.path.exists(prev_log):
            with zipfile.ZipFile(prev_log + ".zip", 'w', zipfile.ZIP_DEFLATED) as zf:
                zf.write(prev_log)
            os.remove(prev_log)
