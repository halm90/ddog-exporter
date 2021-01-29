"""
Influx exporter, datadog helper


Note(s):
  1. Requires Python 3
  2.  Datadog time series query:
      https://docs.datadoghq.com/api/?lang=python#query-time-series-points
    {'status': 'ok',
     'res_type': 'time_series',
     'series': [{'end': 1234,
                 'metric': 'metric name', 'interval': 30,
                 'start': 1234,
                 'length': (length of pointlist),
                 'aggr': None,
                 'attributes': {},
                 'pointlist': [
                     [1430312070000.0, 75.08000183105469],
                            ...
                 ],
                 'expression': '...',
                 'scope': '...',
                 'unit': None,
                 'display_name': '...'
               }],
     'from_date': 1430309983000,
     'group_by': ['host'],
     'to_date': 1430313583000,
     'query': 'system.cpu.idle{*}by{host}',
     'message': u''
    }

"""

from commonpy.logger import Logger
import commonpy.parameters
import datadog


class Dogger(object):  # (datadog.api):
    def __init__(self):
        """
        Initialize the DataDog API object.

        """
        super().__init__()
        self.params = commonpy.parameters.SysParams().params
        self.logger = Logger().logger

        datadog_options = {'api_key': self.params['datadog_api_key'],
                           'app_key': self.params['datadog_app_key'],
                          }

        datadog.initialize(**datadog_options)

    def metrics(self, start, end, query):
        """
        Generator function, returns entries in metrics.

        :param start:
        :param end:
        :param query:
        """
        try:
            self.logger.debug("Datadog query %d - %d: %s", start, end, query)
            dd_metrics = datadog.api.Metric.query(start=start,
                                                  end=end,
                                                  query=query)
        except Exception as exn:
            self.logger.error("Datadog query failed: %s", exn)
        else:
            try:
                for series in dd_metrics['series']:
                    yield series
            except KeyError:
                self.logger.info("Query result not a series: %s", query)
