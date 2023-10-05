from logging import Logger as LogginLogger, Formatter, StreamHandler, INFO
import sys
from functools import partial, wraps
from inspect import *
from .spark_handler import SparkHandler
from .sqs_handler import SQSHandler


class Logger(LogginLogger):
    def __init__(self, name, level=INFO, stream=sys.stdout,
                 format_logger="[%(levelname)s]|%(asctime)s|%(name)s|%(message)s"):
        LogginLogger.__init__(self=self, name=name)
        self.format = format_logger
        self.level = level
        self.setLevel(self.level)
        self.stream = StreamHandler(stream)
        self.stream.setLevel(INFO)
        self.stream.setFormatter(fmt=Formatter(self.format))
        self.addHandler(self.stream)
        self.__sparklog = False
        self.__auditlog = False
        self.__sqslog = False

    def add(self, handler, *args, **kwargs):
        new_handler = handler(*args, **kwargs)
        new_handler.setLevel(self.level)
        new_handler.setFormatter(fmt=Formatter(self.format))
        self.addHandler(new_handler)

    def remove(self, handler):
        self.removeHandler(handler)

    def sparklog(self, *args, **kwargs):
        if self.__sparklog:
            self.__sparklog = False
            self.remove(handler=SparkHandler)
            return self.__sparklog

        else:
            self.__sparklog = True
            self.add(handler=SparkHandler, *args, **kwargs)
            return self.__sparklog

    def sqslog(self, *args, **kwargs):
        if self.__sqslog:
            self.__sqslog = False
            self.remove(handler=SQSHandler)
            return self.__sqslog
        else:
            self.__sqslog = True
            self.add(handler=SQSHandler, *args, **kwargs)
            return self.__sqslog

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
                        self.info(f"Arg: {str(message)}")
                if iargs:
                    for vargs in iargs:
                        message = {
                            "FuncName": f"{func.__name__}",
                            "ArgName": f"{vargs}",
                            "ArgValue": f"{precalled[vargs]}"
                        }
                        self.info(f"Arg: {str(message)}")
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
                        self.info(f"Arg: {str(message)}")
                response = func(*args, **kwargs)
                if rt:
                    allargs = list(precalled.keys())
                    if 'self' in allargs:
                        pass
                    else:
                        self.info(f"Return: {response}")
            except Exception as err:
                self.error(f"{err}")
                raise err

        return wrapper

    def close(self):
        for handler in self.handlers:
            self.remove(handler=handler)
