"""
Unit tests for the datadog-exporter dogger module
"""
from mock import patch, MagicMock, PropertyMock
import pytest
import unittest

import dogger


class TestObject(unittest.TestCase):
    """
    Test basic operation of the main code body.
    """
    _env_dict = {'datadog_api_key': 'datadog api key',
                 'datadog_app_key': 'datadog app key',
                }
    def setUp(self):
        """
        Test setups: patch out functions across all tests.
        """
        patch('commonpy.logger.Logger.logger').start()
        patch('commonpy.parameters.SysParams.params',
              new_callable=PropertyMock,
              return_value=self._env_dict).start()
        self.mock_dog = patch('datadog.initialize').start()

    def tearDown(self):
        """
        Test teardowns: clean up test-wide patches.
        """
        patch.stopall()

    def testInit(self):
        """
        Test the initializer.
        """
        init_opts = {'api_key': self._env_dict['datadog_api_key'],
                     'app_key': self._env_dict['datadog_app_key'],
                    }

        dogger.Dogger()
        self.mock_dog.assert_called_once_with(**init_opts)

    def testMetricsSuccess(self):
        """
        Test the metrics function, successful path
        """
        data = [1, 2, 3, 4]
        metrics = {'series': data}
        with patch('datadog.initialize') as mock_dog, \
             patch('datadog.api.metrics.Metric.query',
                   return_value=metrics) as mock_query, \
             patch('commonpy.parameters.SysParams.params',
                   new_callable=PropertyMock,
                   return_value=self._env_dict):
            dogobj = dogger.Dogger()
            res = [x for x in dogobj.metrics('start', 'end', 'query')]
        mock_query.assert_called_once_with(start='start',
                                           end='end',
                                           query='query')
        self.assertEqual(data, res)

    def testMetricsQueryExcept(self):
        """
        Test the metrics function with datadog query exception
        """
        with patch('datadog.api.Metric.query',
                   side_effect=Exception) as mock_query:
            dogobj = dogger.Dogger()
            res = [x for x in dogobj.metrics('start', 'end', 'query')]
        mock_query.assert_called_once_with(start='start',
                                           end='end',
                                           query='query')
        self.assertEqual([], res)

    def testMetricsNoSeries(self):
        """
        Test the metrics function with datadog query returning not a series
        """
        with patch('datadog.api.Metric.query',
                   side_effect=[{'a': 1},]) as mock_query:
            dogobj = dogger.Dogger()
            res = [x for x in dogobj.metrics('start', 'end', 'query')]

        mock_query.assert_called_once_with(start='start',
                                           end='end',
                                           query='query')
        self.assertEqual([], res)
