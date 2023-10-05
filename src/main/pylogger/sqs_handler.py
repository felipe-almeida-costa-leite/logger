import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import pytz
import datetime
from typing import Dict
import boto3

TZ = "America/Sao_Paulo"
REGION = "us-east-1"


def ssm():
    session = boto3.session.Session()
    client = session.client(service_name="ssm", region_name=REGION)
    return client


def get_parameter(name):
    client = ssm()
    get_secret_value_response = client.get_parameter(Name=name)
    parameter = get_secret_value_response["Parameter"].get("Value")
    return parameter


def sqs():
    session = boto3.session.Session()
    client = session.client(service_name="sqs", region_name=REGION)
    return client


def send_message(queue, message):
    client = sqs()
    response = client.send_message(QueueUrl=queue, MessageBody=message)
    return response


class SQSHandler(logging.Handler):
    def __init__(self, queuelog=None):
        self.queuelog = queuelog
        self.sender = SQSSender(queuelog=self.queuelog)
        logging.Handler.__init__(self=self)

    def emit(self, record: logging.LogRecord):
        formated_message: Dict = {
            "processingDate": f"{datetime.datetime.now(tz=pytz.timezone(TZ)).date().isoformat()}",
            "createdAt": f"{datetime.datetime.now(tz=pytz.timezone(TZ))}",
            "level": f"{record.levelname.lower()}",
            "message": f"{record.getMessage()}"
        }
        self.sender.write(formated_message)


class SQSSender:
    def __init__(self, queuelog=None):
        self.__queuelog = queuelog

    @property
    def queuelog(self):
        if not self.__queuelog:
            self.__queuelog = get_parameter("SQS_LOG_URL")
        return self.__queuelog

    def write(self, msg: Dict):
        send_message(queue=self.queuelog, message=json.dumps(msg))
