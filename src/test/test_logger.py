import unittest
from src.pylogger.logger import Logger


class LoggerTestCase(unittest.TestCase):
    def setUp(self):
        self.logger = Logger(__file__)

    def tearDown(self) -> None:
        self.logger.close()

    def test_streamhandler(self):
        found_stream = False
        for handler in self.logger.handlers:
            if "StreamHandler" in str(handler):
                found_stream = True
        self.assertTrue(found_stream)

    def test_streamhandler_info(self):
        messages = ["MessageTest1", "MessageTest2"]
        with self.assertLogs(self.logger, level="INFO") as stream:
            for message in messages:
                self.logger.info(message)
        for record in stream.records:
            self.assertIn(record.getMessage(), messages)

    def test_decorator_logger(self):
        messages = [
            "Arg: {'FuncName': 'main', 'ArgName': 'parameter', 'ArgValue': 'testparameter'}",
            "MessageTestInner",
            "Return: testparameter"
        ]

        @self.logger
        def main(parameter):
            self.logger.info("MessageTestInner")
            return parameter

        with self.assertLogs(self.logger, level="INFO") as stream:
            main("testparameter")

        for record in stream.records:
            self.assertIn(record.getMessage(), messages)

    def test_decorator_logger_nonrt(self):
        messages = [
            "Return: testparameter"
        ]

        @self.logger(rt=False)
        def main(parameter):
            self.logger.info("MessageTestInner")
            return parameter

        with self.assertLogs(self.logger, level="INFO") as stream:
            main("testparameter")

        for record in stream.records:
            self.assertNotIn(record.getMessage(), messages)

    def test_decorator_logger_nonlargs(self):
        messages = [
            "Arg: {'FuncName': 'main', 'ArgName': 'parameter', 'ArgValue': 'testparameter'}"
        ]

        @self.logger(largs=False)
        def main(parameter):
            self.logger.info("MessageTestInner")
            return parameter

        with self.assertLogs(self.logger, level="INFO") as stream:
            main("testparameter")

        for record in stream.records:
            self.assertNotIn(record.getMessage(), messages)

    def test_decorator_logger_iargs(self):
        messages = [
            "Arg: {'FuncName': 'main', 'ArgName': 'parameter', 'ArgValue': 'testparameter'}",
            "MessageTestInner",
            "Return: ('testparameter', 'testparameter2')"
        ]

        @self.logger(iargs=["parameter"])
        def main(parameter, second):
            self.logger.info("MessageTestInner")
            return parameter, second

        with self.assertLogs(self.logger, level="INFO") as stream:
            main("testparameter", "testparameter2")

        for record in stream.records:
            self.assertIn(record.getMessage(), messages)

    def test_decorator_logger_eargs(self):
        messages = [
            "Arg: {'FuncName': 'main', 'ArgName': 'parameter', 'ArgValue': 'testparameter'}",
        ]

        @self.logger(eargs=["parameter"])
        def main(parameter, second):
            self.logger.info("MessageTestInner")
            return parameter, second

        with self.assertLogs(self.logger, level="INFO") as stream:
            main("testparameter", "testparameter2")

        for record in stream.records:
            self.assertNotIn(record.getMessage(), messages)


if __name__ == '__main__':
    unittest.main()
