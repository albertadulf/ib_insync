from functools import partial
import logging
import logging.handlers
import os
import time

from app.utils.common_util import unique_id


class Log(object):
    """This class is used to log
    """
    log_format = '%(levelname)-8s %(asctime)s  %(message)s'
    global_log_file = 'running'
    global_instance = None

    @staticmethod
    def path(filename=None):
        path = os.getcwd()
        root = 'ib_insync'
        path = path[:path.find(root)] + root + '/logs/'
        if filename is not None:
            path += filename
        return path

    @staticmethod
    def instance():
        if Log.global_instance is not None:
            return Log.global_instance
        Log.global_instance = Log.create(Log.path(Log.global_log_file))
        return Log.global_instance

    @staticmethod
    def create(filename, loglevel=logging.INFO,
               console_enable=True, console_level=logging.INFO):
        self = Log(filename, loglevel, console_enable, console_level)
        return self

    @staticmethod
    def _initialize_handler(handler, loglevel, logformat):
        handler.setLevel(loglevel)
        handler.setFormatter(logging.Formatter(logformat))
        return handler

    @staticmethod
    def _get_valid_log_level(loglevel):
        if isinstance(loglevel, int):
            return loglevel
        if isinstance(loglevel, str):
            level = getattr(logging, loglevel.upper(), None)
            return level
        return None

    def __init__(self, filename, loglevel, console_enable, console_level):
        self._logger = logging.getLogger(unique_id())
        self._console_level = console_level
        self._console_enable = console_enable
        self._console = logging.StreamHandler()
        Log._initialize_handler(
            self._console, self._console_level, Log.log_format)
        self._file = logging.handlers.TimedRotatingFileHandler(
            filename, when='D', interval=1)
        self._file.suffix = '%Y%m%d_%H%M%S.log'
        self._file_level = loglevel
        Log._initialize_handler(
            self._file, self._file_level, Log.log_format)
        self.enable_console(self._console_enable)
        self._logger.addHandler(self._file)
        self._update_logger_level()

    def _update_logger_level(self):
        if self._console_enable:
            level = min(self._console_level, self._file_level)
        else:
            level = self._file_level
        self._logger.setLevel(level)

    def enable_console(self, enable):
        if enable:
            self._logger.addHandler(self._console)
        else:
            self._logger.removeHandler(self._console)
        self._update_logger_level()

    def set_console_level(self, loglevel):
        level = Log._get_valid_log_level(loglevel)
        if level is not None:
            self._console_level = level
            self._console.setLevel(level)
        self._update_logger_level()

    def set_log_file(self, file_name):
        if self._file is not None:
            self._logger.removeHandler(self._file)
        self._file = logging.handlers.TimedRotatingFileHandler(
            file_name, when='D', interval=1)
        self._file.suffix = '%Y%m%d_%H%M%S.log'
        Log._initialize_handler(
            self._file, self._file_level, Log.log_format)
        self._logger.addHandler(self._file)

    def set_log_level(self, loglevel):
        level = Log._get_valid_log_level(loglevel)
        if level is not None:
            self._file_level = level
            self._file.setLevel(level)
        self._update_logger_level()

    def get_logger(self, tag):
        return Log.TagLogger(self._logger, tag)

    class TagLogger(object):
        def __init__(self, logger, tag=None):
            self._logger = logger
            self._tag = tag

        def __getattr__(self, func):
            if func.lower() in (
                    'debug', 'info', 'warning', 'error', 'critical'):
                method = getattr(self._logger, func.lower())
                return partial(self._customized_log, method)
            return None

        def _customized_log(self, func, log, *args):
            if self._tag is not None:
                func('[%s] ' + log, self._tag, *args)
            else:
                func(log, *args)


def test():
    instance = Log.instance()
    logger = instance.get_logger('test')
    logger.info('hello')
    logger.debug('not out')
    instance.set_log_level('debug')
    logger.debug('actually out')
    instance.set_log_file('test2.log')
    logger.error('actually out2')
    logger.info('hello %d %s', 123, '456')
    instance2 = Log.create('test3.log')
    logger2 = instance2.get_logger('test2')
    logger2.error('actually out3')
    logger2.info(os.getcwd())
    time.sleep(6)
    logger.info('after sleep')
    logger.debug('great')
    logger2.warning('greattt')


if __name__ == '__main__':
    test()
