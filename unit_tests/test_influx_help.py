"""
Unit tests for the datadog-exporter influx helper module
"""
from mock import patch, MagicMock, PropertyMock
import pytest
import unittest
import influxdb

import influx_help


class TestObject(unittest.TestCase):
    """
    Test basic operation of the main code body.
    """
    _env_dict = {'influx_database': 'some database',
                 'influx_host': 'a host',
                 'influx_port': 'some port',
                 'influx_user': 'joe',
                 'influx_password': 'secret word',
                 'influx_timeout': '1 year',
                 'START_TIMESTAMP': 'yesterday',
                }
    test_info = ('foundation name', {'environment': 'green',
                                     'dc': 'ac',
                                     'region': 'out there',
                                     'context': 'testing',
                                    })

    def setUp(self):
        """
        Test setups: patch out functions across all tests.
        """
        patch('commonpy.logger.Logger.logger').start()
        patch('commonpy.parameters.SysParams.params',
              new_callable=PropertyMock,
              return_value=self._env_dict).start()

    def tearDown(self):
        """
        Test teardowns: clean up test-wide patches.
        """
        patch.stopall()

    def testInit(self):
        """
        Test the initializer.
        """
        with patch('influxdb.InfluxDBClient') as mock_client:
            influx_help.InfluxHelper()
        mock_client.assert_called_once_with(host=self._env_dict['influx_host'],
                                            port=self._env_dict['influx_port'],
                                            username=self._env_dict['influx_user'],
                                            password=self._env_dict['influx_password'],
                                            timeout=self._env_dict['influx_timeout'],
                                            database=self._env_dict['influx_database'])

    def testIsNumberOK(self):
        """
        Test the is_number static method with a valid number
        """
        self.assertTrue(influx_help.InfluxHelper.is_number(42))
        self.assertTrue(influx_help.InfluxHelper.is_number(42.42))
        self.assertTrue(influx_help.InfluxHelper.is_number("42"))

    def testIsNumberNaN(self):
        """
        Test the is_number static method with an invalid number
        """
        self.assertFalse(influx_help.InfluxHelper.is_number("my name is joe"))
        self.assertFalse(influx_help.InfluxHelper.is_number([]))
        self.assertFalse(influx_help.InfluxHelper.is_number({}))

    def testMetricStartTimeSuccess(self):
        """
        Validate success path through get_metric_start_time()
        """
        class QueryReturn:
            raw = {'series': [{'values': [[1000]]}]}
        query_return = QueryReturn
        raw_return = "back from raw"
        sql = 'SELECT LAST(*) FROM \"a metric\"'
        metric = 'a metric'

        with patch('influxdb.InfluxDBClient'):
            helper = influx_help.InfluxHelper()
            helper.database = 'my data'
            helper.dbclient.query.return_value=query_return
            res = helper.get_metric_start_time(metric)

        helper.dbclient.query.assert_called_once_with(sql,
                                           database='my data',
                                           epoch="ms")
        self.assertEqual(1, res)

    def testMetricStartTimeNoSeries(self):
        """
        Validate get_metric_start_time() with non-series data returned
        """
        metric = 'a metric'
        sql = 'SELECT LAST(*) FROM \"a metric\"'
        with patch('influxdb.InfluxDBClient'), \
             pytest.raises(influx_help.InfluxStartQueryFailed):
            helper = influx_help.InfluxHelper()
            helper.database = 'my data'
            helper.dbclient.query.return_value = {}
            res = helper.get_metric_start_time(metric)
        helper.dbclient.query.assert_called_once_with(sql,
                                                      database='my data',
                                                      epoch="ms")

    def testMetricStartTimeException(self):
        """
        Validate get_metric_start_time() with query raising exception
        """
        metric = 'a metric'
        sql = 'SELECT LAST(*) FROM \"a metric\"'
        with patch('influxdb.InfluxDBClient'), \
             pytest.raises(influx_help.InfluxStartQueryFailed):
            helper = influx_help.InfluxHelper()
            helper.database = 'my data'
            helper.dbclient.query.side_effect = Exception
            res = helper.get_metric_start_time(metric)
        helper.dbclient.query.assert_called_once_with(sql,
                                                      database='my data',
                                                      epoch="ms")

    def testSendPointsOnePoint(self):
        """
        Validate send_points() success path sending 1 point
        """
        metric = 'a metric'
        points = [(4, 2)]
        with patch('influxdb.InfluxDBClient'), \
             patch('influxdb.SeriesHelper.commit') as mock_commit, \
             patch('influx_help.InfluxHelper.is_number', return_value=True):
            helper = influx_help.InfluxHelper()
            helper.dbclient = MagicMock()
            helper.send_points(metric, self.test_info, points)
        mock_commit.assert_called_once()

    def testSendPointsMultiPoint(self):
        """
        Validate send_points() success path sending multiple point
        """
        metric = 'a metric'
        points = [('1', 2), ('3', 4), ('5', 6), ('7', 8)]
        with patch('influxdb.InfluxDBClient'), \
             patch('influxdb.SeriesHelper.commit') as mock_commit, \
             patch('influx_help.InfluxHelper.is_number', return_value=True):
            helper = influx_help.InfluxHelper()
            helper.dbclient = MagicMock()
            helper.send_points(metric, self.test_info, points)
        mock_commit.assert_called_once()

    def testSendPointsNoPoints(self):
        """
        Validate send_points() sending no points (empty point list)
        """
        metric = 'a metric'
        points = []
        with patch('influxdb.InfluxDBClient'), \
             patch('influxdb.SeriesHelper.commit') as mock_commit, \
             patch('influx_help.InfluxHelper.is_number', return_value=True):
            helper = influx_help.InfluxHelper()
            helper.dbclient = MagicMock()
            helper.send_points(metric, self.test_info, points)
        mock_commit.assert_called_once()

    def testSendPointsPointError(self):
        """
        Validate send_points() point constructed incorrectly
        """
        metric = 'a metric'
        points = [('99',)]
        with patch('influxdb.InfluxDBClient'), \
             patch('influxdb.SeriesHelper.commit') as mock_commit, \
             patch('influx_help.InfluxHelper.is_number', return_value=True):
            helper = influx_help.InfluxHelper()
            helper.dbclient = MagicMock()
            helper.send_points(metric, self.test_info, points)
        mock_commit.assert_not_called()

    def testSendPointsCommitException(self):
        """
        Validate send_points() commit call raises exception
        """
        metric = 'a metric'
        points = [('4', 2)]
        with patch('influxdb.InfluxDBClient'), \
             patch('influxdb.SeriesHelper.commit',
                   side_effect=Exception) as mock_commit, \
             patch('influx_help.InfluxHelper.is_number', return_value=True):
            helper = influx_help.InfluxHelper()
            helper.dbclient = MagicMock()
            helper.send_points(metric, self.test_info, points)
        mock_commit.assert_called_once()
