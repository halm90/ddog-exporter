"""
Influx DB helper class

Note(s):
    1. Requires Python 3
    2. The SeriesHelper is an influxdb.SeriesHelper with Meta subclass
       Refer to Influx DB documentation
         See http://influxdb-python.readthedocs.io/en/latest/api-documentation.html
         Meta class stores time series helper configuration.

"""
import influxdb

from commonpy.logger import Logger
from commonpy.parameters import SysParams


class InfluxStartQueryFailed(Exception):
    """ The start-time query failed """


class InfluxHelper(object):
    """
    Influxdb helper class
    """
    def __init__(self):
        """
        Influxdb helper class initializer
        """
        super().__init__()
        self.params = SysParams().params
        self.database = self.params['influx_database']
        self.logger = Logger().logger

        # The client should be an instance of InfluxDBClient.
        self.dbclient = influxdb.InfluxDBClient(host=self.params['influx_host'],
                                                port=self.params['influx_port'],
                                                username=self.params['influx_user'],
                                                password=self.params['influx_password'],
                                                timeout=self.params['influx_timeout'],
                                                database=self.database)

    @staticmethod
    def is_number(nbr):
        """
        Determine if the given string (or number) can be converted to
        a float (in other words, is a valid number).

        :param n: variable to check
        :return: True if number else false
        """
        try:
            float(nbr)
        except (ValueError, TypeError):
            return False
        else:
            return True

    def get_metric_start_time(self, metric):
        """
        Get the start time for the given influxdb metric.

        :param metric:
        :return: start time
        """
        start = self.params['START_TIMESTAMP']
        try:
            influx_query = "SELECT LAST(*) FROM \"{}\"".format(metric)
            self.logger.debug("Influx query: %s", influx_query)
            influx_result = self.dbclient.query(influx_query,
                                                database=self.database,
                                                epoch="ms")
            start = int(influx_result.raw['series'][0]['values'][0][0] / 1000)
        except (KeyError, IndexError) as exn:
            self.logger.error('Key/Index error in Influx result: %s', exn)
        except Exception as exn:
            self.logger.debug(("Unable to retrieve last timestamp from InfluxDB "
                               "for {}: error {}").format(influx_query, exn))
            self.logger.warn("Setting start timestamp to {0}".format(start))
        else:
            # no exception, return start time
            return start
        # Any exception: raise 'failed'
        raise InfluxStartQueryFailed

    def send_points(self, metric, foundation_info, points):
        """
        Send the point series to influx

        :param foundry:
        :param points: list of points (as [time, value])
        :return:

        Note(s):
          1. points are returned from Datadog query
             https://docs.datadoghq.com/api/?lang=python#query-time-series-points
        """
        class SeriesHelper(influxdb.SeriesHelper):
            class Meta:
                # The client should be an instance of InfluxDBClient.
                client = self.dbclient
                # Defines all the fields in this time series.
                fields = ['value', 'time']
                # Defines all the tags for the series.
                tags = ['foundry', 'environment', 'dc', 'region', 'context']
                # Defines the number of data points to store prior to writing
                # on the wire.
                bulk_size = 150
                # autocommit must be set to True when using bulk_size
                autocommit = True

        SeriesHelper.Meta.series_name = metric
        (foundry, info) = foundation_info
        for pnbr, point in enumerate(points, 1):
            try:
                point_time, point_value = point[0:2]
            except:
                self.logger.warning("Unable to determine time/value on metrics: %s",
                                    point)
                return
            # SeriesHelper(....)
            if self.is_number(point_value):
                self.logger.debug("Influx: [%d] send point [%d : %d]",
                                  pnbr, point_time, point_value)
                try:
                    SeriesHelper(foundry=foundry,
                                 environment=info['environment'],
                                 dc=info['dc'],
                                 region=info['region'],
                                 context=info['context'],
                                 time=int(point_time) * 1000000,
                                 value=point_value)
                except Exception as exn:
                    self.logger.warning("Influx: error sending point %d %s: %s",
                                        pnbr, str(point), type(exn))
                    self.logger.debug("Influx error sending point %d: %s",
                                      pnbr, exn)
                    return
        try:
            self.logger.debug("Influx: commit")
            SeriesHelper.commit()
            self.logger.debug("Influx JSON: %s", SeriesHelper._json_body_())
        except Exception as exn:
            self.logger.warn("InfluxDB commit failed: %s", exn)
