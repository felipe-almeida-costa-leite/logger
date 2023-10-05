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


class SparkHandler(logging.Handler):
    def __init__(self, spark=None, logtable=None):
        self.spark = spark
        self.logtable = logtable
        self.sender = SparkSender(spark=self.spark, logtable=self.logtable)
        logging.Handler.__init__(self=self)

    def emit(self, record: logging.LogRecord):
        formated_message: Dict = {
            "processingDate": f"{datetime.datetime.now(tz=pytz.timezone(TZ)).date().isoformat()}",
            "createdAt": f"{datetime.datetime.now(tz=pytz.timezone(TZ))}",
            "level": f"{record.levelname.lower()}",
            "message": f"{record.getMessage()}"
        }
        self.sender.write(formated_message)


class SparkSender:
    def __init__(self, spark=None, logtable=None):
        self.__spark = spark
        self.__logtable = logtable

    @property
    def spark(self):
        if not self.__spark:
            self.__spark = (
                SparkSession.builder
                .master("local")
                .appName("SparkHandler")
                .enableHiveSupport()
                .getOrCreate()
            )
        return self.__spark

    @property
    def logtable(self):
        if not self.__logtable:
            self.__logtable = get_parameter("TABLE_LOG")
        return self.__logtable

    @property
    def schema(self):
        schema = StructType()
        schema.add(field="processingDate", data_type="date")
        schema.add(field="createdAt", data_type="timestamp")
        schema.add(field="level", data_type="string")
        schema.add(field="message", data_type="string")
        return schema

    def write(self, msg: Dict):
        dfLog = self.spark.createDataFrame(data=msg, schema=self.schema)
        dfLog.createOrReplaceTempView("DFLogs")
        date_processing = f"{datetime.datetime.now(tz=pytz.timezone(TZ)).date().isoformat()}"
        self.session.sql(
            f"INSERT INTO {self.logtable} PARTITION ( processingDate = '{date_processing}') SELECT * FROM DFLogs")

