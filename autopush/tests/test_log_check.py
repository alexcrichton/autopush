import json

import twisted
from moto import mock_dynamodb2
from nose.tools import eq_, ok_
from twisted.internet.defer import inlineCallbacks
from twisted.logger import globalLogPublisher
from twisted.trial import unittest

from autopush.db import create_rotating_message_table
from autopush.http import EndpointHTTPFactory
from autopush.logging import begin_or_register
from autopush.settings import AutopushSettings
from autopush.tests.client import Client
from autopush.tests.support import TestingLogObserver
from autopush.web.log_check import LogCheckHandler

mock_dynamodb2 = mock_dynamodb2()


def setUp():
    mock_dynamodb2.start()
    create_rotating_message_table()


def tearDown():
    mock_dynamodb2.stop()


class LogCheckTestCase(unittest.TestCase):

    def setUp(self):
        twisted.internet.base.DelayedCall.debug = True

        settings = AutopushSettings(
            hostname="localhost",
            statsd_host=None,
        )

        self.logs = TestingLogObserver()
        begin_or_register(self.logs, discardBuffer=True)
        self.addCleanup(globalLogPublisher.removeObserver, self.logs)

        app = EndpointHTTPFactory.for_handler(LogCheckHandler, settings)
        self.client = Client(app)

    @inlineCallbacks
    def test_get_err(self):
        resp = yield self.client.get('/v1/err')
        eq_(len(self.logs), 2)
        ok_(self.logs.logged(
            lambda e: (e['log_level'].name == 'error' and
                       e['log_format'] == 'Test Error Message' and
                       e['status_code'] == 418)
        ))
        payload = json.loads(resp.content)
        eq_(payload.get('code'), 418)
        eq_(payload.get('message'), "ERROR:Success")

    @inlineCallbacks
    def test_get_crit(self):
        resp = yield self.client.get('/v1/err/crit')
        eq_(len(self.logs), 2)
        ok_(self.logs.logged(
            lambda e: (e['log_level'].name == 'critical' and
                       e['log_failure'] and
                       e['log_format'] == 'Test Critical Message' and
                       e['status_code'] == 418)
        ))
        payload = json.loads(resp.content)
        eq_(payload.get('code'), 418)
        eq_(payload.get('error'), "Test Failure")

        self.flushLoggedErrors()
