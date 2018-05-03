import os
import psutil
import pandas as pd
import logging
import time
import codecs
from logging.handlers import TimedRotatingFileHandler
from config import STATIC_CONFIG
import datetime as dt

logger=None

class MyTimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
    def __init__(self, dir_log):
        self.dir_log = dir_log
        filename = self.dir_log + "tp2_" + time.strftime("%m%d%y") + ".log"
        logging.handlers.TimedRotatingFileHandler.__init__(self, filename,
                                                           when='midnight', interval=1, backupCount=0, encoding=None)

    def doRollover(self):
        """
        TimedRotatingFileHandler remix - rotates logs on daily basis, and
        filename of current logfile is "tp2_" + time.strftime("%m%d%y") + ".log" always
        """
        self.stream.close()
        # get the time that this sequence started at and make it a TimeTuple
        t = self.rolloverAt - self.interval
        timeTuple = time.localtime(t)
        self.baseFilename = self.dir_log + "tp2_" + time.strftime("%m%d%y") + ".log"

        if self.encoding:
            self.stream = codecs.open(self.baseFilename, 'w', self.encoding)
        else:
            self.stream = open(self.baseFilename, 'w')

        self.rolloverAt = self.rolloverAt + self.interval

class MyFormatter(logging.Formatter):
    converter=dt.datetime.fromtimestamp
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s,%03d" % (t, record.msecs)
        return s

def read_config():
    path_to_config = os.environ['TP2_HOME']
    # path is in tp2, which is one level above and can be represented as ..
    # remaining path can be supplied to os.path
    _path = os.path.join(path_to_config, 'data', 'inputs', 'settings.csv')
    df = pd.read_csv(_path)
    config = {}
    for row in df.itertuples():
        tmp = config.get(row.Type, {})
        tmp.update({row.Key: row.Value})
        config[row.Type] = tmp

    return config


def setup_logging(model_log, level='info'):
    """ Carries out the setup for
    the logging function
    """
    logger = logging.getLogger()
    handler = MyTimedRotatingFileHandler(model_log)
    # handler.suffix = "%m%d%Y %M.log"
    #formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    formatter = MyFormatter(fmt='%(asctime)s %(message)s', datefmt='%Y-%m-%d,%H:%M:%S.%f')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    if level.lower() == 'debug':
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # logger.info("Logging setup complete")
    return logger

config = read_config()
error_log_path = os.environ['LOGDIR'] + STATIC_CONFIG['PATHS']["error_log_path"]
logging_info = config.get('LOGGING')
level = logging_info.get('level', 'info')

if logger is None:
    logger = setup_logging(error_log_path, level=level)

# decorator, in debug as default mode so that it wont throw un-necessary logs, if the settings.csv says debug it will
# throw debug else its info only so you wont see this.
def log_function(func):
    def wrapped(*args, **kwargs):
        process = psutil.Process(os.getpid())
        logger.debug("**********Started function {0}, memory in MB={1}".format(func.__name__, process.memory_info().rss/(1024*1024.0)))
        return func(*args, **kwargs)
    return wrapped