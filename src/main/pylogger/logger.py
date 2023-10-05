import logging
import sys
from functools import partial, wraps
from inspect import *
from .spark_handler import SparkHandler
from .spark_audit import SparkHandlerAudit
from .sqs_handler import SQSHandler


class Logger(object):
    def __init__(self, name, level=logging.INFO, stream=sys.stdout,
                 format="[%(levelname)s]|%(asctime)s|%(name)s|%(message)s"):
        self.format = format
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.stream = logging.StreamHandler(stream)
        self.stream.setLevel(logging.INFO)
        self.stream.setFormatter(fmt=logging.Formatter(self.format))
        self.logger.addHandler(self.stream)
        self.__sparklog = False
        self.__auditlog = False
        self.__sqslog = False

    def add(self, handler, *args, **kwargs):
        new_handler = handler(*args, **kwargs)
        new_handler.setLevel(logging.INFO)
        new_handler.setFormatter(fmt=logging.Formatter(self.format))
        self.logger.addHandler(new_handler)

    def remove(self, handler):
        self.logger.removeHandler(handler)

    def sparklog(self, *args, **kwargs):
        if self.__sparklog:
            self.__sparklog = False
            self.remove(handler=SparkHandler)
            return self.__sparklog

        else:
            self.__sparklog = True
            self.add(handler=SparkHandler, *args, **kwargs)
            return self.__sparklog

    def auditlog(self, *args, **kwargs):
        if self.__auditlog:
            self.__auditlog = False
            self.remove(handler=SparkHandlerAudit)
            return self.__auditlog
        else:
            self.__auditlog = True
            self.add(handler=SparkHandlerAudit, *args, **kwargs)
            return self.__auditlog

    def sqslog(self, *args, **kwargs):
        if self.__sqslog:
            self.__sqslog = False
            self.remove(handler=SQSHandler)
            return self.__sqslog
        else:
            self.__sqslog = True
            self.add(handler=SQSHandler, *args, **kwargs)
            return self.__sqslog

    def debug(self, msg: str):
        self.logger.debug(msg)

    def info(self, msg: str):
        self.logger.info(msg)

    def warning(self, msg: str):
        self.logger.warning(msg)

    def error(self, msg: str):
        self.logger.error(msg)

    def __call__(self, func=None, rt=True, iargs=None, eargs=None, largs=True):
        if func is None:
            return partial(self.__call__, rt=rt, iargs=iargs, eargs=eargs, largs=largs)

        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                precalled = getcallargs(func, *args, **kwargs)
                if largs and not iargs and not eargs:
                    allargs = list(precalled.keys())
                    if 'self' in allargs:
                        allargs.remove('self')
                    for vargs in allargs:
                        message = {
                            "FuncName": f"{func.__name__}",
                            "ArgName": f"{vargs}",
                            "ArgValue": f"{precalled[vargs]}"
                        }
                        self.info(f"LogArgs: {str(message)}")
                if iargs:
                    for vargs in iargs:
                        message = {
                            "FuncName": f"{func.__name__}",
                            "ArgName": f"{vargs}",
                            "ArgValue": f"{precalled[vargs]}"
                        }
                        self.info(f"LogArgs: {str(message)}")
                if eargs:
                    set_precalled_keys = set(precalled.keys())
                    set_eargs = set(eargs)
                    dif = list(set_precalled_keys - set_eargs)
                    for vargs in dif:
                        message = {
                            "FuncName": f"{func.__name__}",
                            "ArgName": f"{vargs}",
                            "ArgValue": f"{precalled[vargs]}"
                        }
                        self.info(f"LogArgs: {str(message)}")
                response = func(*args, **kwargs)
                if rt:
                    allargs = list(precalled.keys())
                    if 'self' in allargs:
                        pass
                    else:
                        self.info(f"LogReturn: {response}")
            except Exception as err:
                self.error(f"{err}")
                raise err

        return wrapper

    def handlers(self):
        return self.logger.handlers

    def close(self):
        for handler in self.handlers():
            self.remove(handler=handler)
