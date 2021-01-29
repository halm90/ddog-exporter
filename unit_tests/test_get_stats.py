"""
Unit tests for the datadog-exporter get_stats module
"""
from mock import patch, MagicMock, PropertyMock, mock_open, call
import pytest
import unittest

import influx_help
import get_stats


class TestSupportFunctions(unittest.TestCase):
    """
    Test basic operation of the assorted functions
    """
    def setUp(self):
        """
        Test setups: patch out functions across all tests.
        """
        patch('commonpy.logger.Logger.logger').start()
        patch('commonpy.parameters.SysParams.params',
              new_callable=PropertyMock).start()

    def tearDown(self):
        """
        Test teardowns: clean up test-wide patches.
        """
        patch.stopall()

    def testGetFoundationObjectFound(self):
        """
        Test get_foundation_object
        """
        fname = 'mumble1'
        good_foundry = {"foundry": fname,
                        "more": "stuff",
                        "in": "here",
                       }
        source_info = {"foundations": [{**good_foundry,
                                       },
                                       {"foundry": "mumble2",
                                        "some": "other",
                                        "stuff": "here",
                                       }
                                      ]
                      }
        foundry, info = get_stats.Exporter().get_foundation_object(fname,
                                                                   source_info)
        self.assertEqual(foundry, fname)
        self.assertEqual(info, good_foundry)

    def testGetFoundationObjectNotFound(self):
        """
        Test get_foundation_object, no matching foundation
        """
        fname = 'mumble1'
        good_foundry = {"foundry": fname,
                        "more": "stuff",
                        "in": "here",
                       }
        source_info = {"foundations": [{**good_foundry,
                                       },
                                       {"foundry": "mumble2",
                                        "some": "other",
                                        "stuff": "here",
                                       }
                                      ]
                      }
        foundry, info = get_stats.Exporter().get_foundation_object("foobar",
                                                                   source_info)
        self.assertEqual(foundry, None)
        self.assertEqual(info, {})

    def testLoadJsonSuccess(self):
        """
        Test load_json_file (success)
        """
        phony_out = "loaded json stuff"
        with patch("builtins.open", mock_open()) as mk_open, \
             patch('json.load', return_value=phony_out) as mock_load:
            result = get_stats.Exporter().load_json_file("some_filename")
        self.assertEqual(result, phony_out)
        mk_open.assert_called_once_with('some_filename', 'r')
        mock_load.assert_called_once()

    def testLoadJsonNoFile(self):
        """
        Test load_json_file (success)
        """
        with patch("builtins.open", mock_open()) as mk_open, \
             pytest.raises(FileNotFoundError), \
             patch('json.load') as mock_load:
            mk_open.side_effect = FileNotFoundError
            result = get_stats.Exporter().load_json_file("some_filename")
        mk_open.assert_called_once_with('some_filename', 'r')
        mock_load.assert_not_called()

    def testLoadJsonFail(self):
        """
        Test load_json_file (success)
        """
        with patch("builtins.open", mock_open()) as mk_open, \
             pytest.raises(Exception), \
             patch('json.load', side_effect=Exception) as mock_load:
            result = get_stats.Exporter().load_json_file("some_filename")
        mk_open.assert_called_once_with('some_filename', 'r')


class TestSendFunctions(unittest.TestCase):
    """
    Test the exporter influx/datadog sending functions
    """
    _env_dict = {'foundations_file': 'found_file',
                 'queries_file': 'query_file',
                 'datadog_time_range': 1,
                 'START_TIMESTAMP': 0,
                }
    def setUp(self):
        """
        Test setups: patch out functions across all tests.
        """
        patch('commonpy.logger.Logger.logger').start()
        patch('commonpy.parameters.SysParams.params',
              new_callable=PropertyMock,
              return_value=self._env_dict).start()
        self.mock_dogger = patch('dogger.Dogger').start()
        self.mock_helper = patch.object(influx_help, 'InfluxHelper',
                                        autospec=True).start()

    def tearDown(self):
        """
        Test teardowns: clean up test-wide patches.
        """
        patch.stopall()

    def testSendResultsSuccess(self):
        """
        Simple success path through send_results()
        """
        metlist = [{'scope': 'a:foundry', 'pointlist': ['123']},]
        def my_metrics(*args, **kwargs):
          for rtn in metlist:
            yield rtn
        with patch('time.time', return_value=1), \
             patch('get_stats.Exporter.get_foundation_object') as mock_get:
            exporter = get_stats.Exporter()
            exporter.datadog.metrics = MagicMock(side_effect=my_metrics)
            exporter.helper.send_points = MagicMock()
            exporter.send_results(0, 'metric', 'why not', 'info')

        mock_get.assert_called_once_with('foundry', 'info')
        exporter.datadog.metrics.assert_called_once_with(0, 1, 'why not')
        exporter.helper.send_points.assert_called_once()

    def testSendResultsFoundationError(self):
        """
        get_foundation_object failure in send_results()
        """
        metlist = [{'scope': 'a:foundry', 'pointlist': ['123']},]
        def my_metrics(*args, **kwargs):
          for rtn in metlist:
            yield rtn
        with patch('time.time', return_value=1), \
             patch('get_stats.Exporter.get_foundation_object',
                   side_effect=IndexError) as mock_get:
            exporter = get_stats.Exporter()
            exporter.datadog.metrics = MagicMock(side_effect=my_metrics)
            exporter.helper.send_points = MagicMock()
            exporter.send_results(0, 'metric', 'why not', 'info')

        mock_get.assert_called_once_with('foundry', 'info')
        exporter.datadog.metrics.assert_called_once_with(0, 1, 'why not')
        exporter.helper.send_points.assert_not_called()


class TestExporterRun(unittest.TestCase):
    """
    Test basic operation of the run() function

    Notes:
        * 1. query_dict KeyError
        X 2. query_dict list
        * 3. query_dict bad keys
          4. get_start_time fail
          5. get_start_time succeed
          6. range loop: datadog.metrics None
          7. range loop: datadog.metrics None
          8. range loop: datadog.metrics list
          9. range loop: metrics list: get_foundation_object indexerror
         10. range loop: metrics list: send_points
    """
    _env_dict = {'foundations_file': 'found_file',
                 'queries_file': 'query_file',
                 'datadog_time_range': 10,
                 'START_TIMESTAMP': 99,
                }
    def setUp(self):
        """
        Test setups: patch out functions across all tests.
        """
        patch('commonpy.logger.Logger.logger').start()
        patch('commonpy.parameters.SysParams.params',
              new_callable=PropertyMock,
              return_value=self._env_dict).start()
        patch('time.time', return_value=0).start()
        self.mock_dogger = patch('dogger.Dogger').start()
        self.mock_helper = patch.object(influx_help, 'InfluxHelper',
                                        autospec=True).start()

    def tearDown(self):
        """
        Test teardowns: clean up test-wide patches.
        """
        patch.stopall()

    def testRunBadQueries(self):
        """
        Test main: queries json/file malformed (missing 'queries' key)
        """
        with patch('get_stats.Exporter.load_json_file',
                   side_effect=['some foundation', {'foo': 'bar'}]):
            res = get_stats.Exporter().run()
        self.assertEqual(1, res)

    def testRunBadQueryBadKeys(self):
        """
        Test main: queries query malformed (with 'queries', missing key(s))
        """
        with patch('get_stats.Exporter.load_json_file',
                   side_effect=['some foundation',
                                  {'queries': [{'q1': 1, 'q2': 2}]}]):
            res = get_stats.Exporter().run()
        self.assertEqual(1, res)

    def testRunSuccess(self):
        """
        Test main: simple success path
        """
        json_load = ['some foundation',
                     {'queries': [{'metric': 'foo', 'query': 'why not'}]}]
        with patch('get_stats.Exporter.send_results') as mock_send, \
             patch('get_stats.Exporter.load_json_file',
                   side_effect=json_load) as mock_load:
            exporter = get_stats.Exporter()
            exporter.helper.get_metric_start_time = MagicMock(return_value=42)
            res = exporter.run()

        mock_send.assert_called_once_with(42, 'foo', 'why not', 'some foundation')
        mock_load.assert_has_calls([call('found_file'), call('query_file')])
        exporter.helper.get_metric_start_time.assert_called_once_with('foo')
        self.assertEqual(res, 0)

    def testRunGetStartTimeFail(self):
        """
        Test main: failure in get_metric_start_time()
        """
        json_load = ['some foundation',
                     {'queries': [{'metric': 'foo', 'query': 'why not'}]}]
        mock_stime = MagicMock(side_effect=influx_help.InfluxStartQueryFailed)
        with patch('get_stats.Exporter.send_results') as mock_send, \
             patch('get_stats.Exporter.load_json_file',
                   side_effect=json_load) as mock_load:
            exporter = get_stats.Exporter()
            exporter.helper.get_metric_start_time = mock_stime
            res = exporter.run()

        mock_send.assert_called_once_with(99, 'foo', 'why not', 'some foundation')
        mock_load.assert_has_calls([call('found_file'), call('query_file')])
        exporter.helper.get_metric_start_time.assert_called_once_with('foo')
        self.assertEqual(res, 0)
