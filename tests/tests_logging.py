import logging
import sys
import os
import unittest


class OsConfLoggingTest(unittest.TestCase):
    def tearDown(self):
        for key in os.environ.keys():
            if key.startswith('SIPPERS_LOGGING'):
                os.environ.pop(key)

    def test_default_level_is_logging(self):
        from sippers import logger
        reload(sys.modules['sippers'])

        self.assertEqual(logger.level, logging.INFO)

    def test_config_level_from_environment(self):
        os.environ['SIPPERS_LOGGING_LEVEL'] = 'DEBUG'
        # Reload the module to recreate sippers.logger
        reload(sys.modules['sippers'])
        from sippers import logger

        self.assertEqual(logger.level, logging.DEBUG)

    def test_config_file_from_environement(self):
        import tempfile
        logfile = tempfile.mkstemp(prefix='sippers-test-')[1]
        os.environ['SIPPERS_LOGGING_LOGFILE'] = logfile
        # Reload the module to recreate sippers.logger
        reload(sys.modules['sippers'])
        from sippers import logger

        logger.info('Foo')
        with open(logfile, 'r') as f:
            logcontent = f.read()
        self.assertRegexpMatches(logcontent, "\[[0-9]{4}-[0-9]{2}-[0-9]{2} "
        "[0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}\] INFO .*: Foo")
