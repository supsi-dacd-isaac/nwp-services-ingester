from logging.handlers import TimedRotatingFileHandler
import zipfile
import os
import time
from glob import glob


class ZippedTimedRotatingFileHandler(TimedRotatingFileHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def doRollover(self):
        # Rotate the log file first
        super().doRollover()

        # get list of files which will be zipped, basically all files with the same baseFilename
        to_zip = glob(self.baseFilename+'.*')
        # delete files ending with .zip
        to_zip = [f for f in to_zip if not f.endswith('.zip')]

        for tz in to_zip:
            with zipfile.ZipFile(tz + ".zip", 'w', zipfile.ZIP_DEFLATED) as zf:
                zf.write(tz)
            os.remove(tz)
