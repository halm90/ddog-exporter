"""
Influx DB exporter

Note(s):
    1. Requires Python 3

"""
import argparse
import json
import time

import constants
import dogger
import influx_help

from commonpy.logger import Logger
from commonpy.parameters import SysParams


class Exporter(object):
    """
    Class encapsulates the main run loop.  Making this a class with
    separate methods facilitates unit testing.
    """
    def __init__(self):
        super().__init__()
        self.logger = Logger().logger
        self.params = SysParams().params
        self.datadog = dogger.Dogger()
        self.helper = influx_help.InfluxHelper()

    def get_foundation_object(self, foundation, foundation_info):
        """
        Get the list of foundation info from the info file, given
        the foundation name.

        :param foundation: the foundation to find in the file info
        :param foundation_info: loaded foundation info file
        :return: foundation info corresponding to requested foundation
        """
        info = (None, {})
        try:
            info = (foundation,
                    [foundry for foundry in foundation_info["foundations"]
                     if foundry["foundry"] == foundation][0])
        except KeyError:
            self.logger.error("Malformed foundation info (dictionary missing keyword)")
        except IndexError:
            self.logger.error("Foundry %s not found in foundation info",
                              foundation)
        return info

    def load_json_file(self, filename):
        """
        Load the named json file.  If the file is not found and readable,
        or the json load fails, then an exception is raised.

        :param filename: name of the json file to read
        :return: the loaded json dictionary
        """
        try:
            self.logger.debug('Load json file: %s', filename)
            payload = json.load(open(filename, 'r'))
        except FileNotFoundError:
            self.logger.error('Error: file not found: %s', filename)
            raise
        except Exception as exn:
            self.logger.error('Error loading json file %s: %s', filename, exn)
            raise
        else:
            return payload

    def send_results(self, start_time, metric, query, foundation_info):
        """
        Send the datadog metric results to Influx

        :param start_time: earliest results to send
        :param metric: the metric being selected
        :param query: the datadog query to use
        :param foundation_info: the foundation description
        """
        now = int(time.time())
        time_range = self.params['datadog_time_range']

        # Datadog returns 150 datapoints per call.  Set the end timeframe
        # <time range> hours in the future or now, whichever is sooner
        end = min(start_time + time_range, now)

        #Loop through datadog results until we reach current time
        for start in range(start_time, now, time_range):
            self.logger.info("Start %d : end %d (diff: %d)",
                             start, end, end-start_time)
            # Execute the datadog query, and for every item in the series,
            # for each point in the item, write the point to influx.
            for series in self.datadog.metrics(start, end, query):
                end = end + time_range
                foundry = series['scope'].split(":")[1]
                points = series['pointlist']
                self.logger.debug('Process %d points for foundry %s',
                                  len(points), foundry)
                try:
                    fnd_info = self.get_foundation_object(foundry, foundation_info)
                except IndexError:
                    self.logger.error("Error retrieving foundation " + \
                                      "info %s.  Ignore results.",
                                      foundry)
                else:
                    # send all points from the current series to influx
                    self.helper.send_points(metric, fnd_info, points)

    def run(self):
        """
        Exporter main run loop.

        :return: status code (1 fail, 0 success)
        """

        # Load info about foundations and metric queries from files
        # An exception will be raised if the load fails.  Not caught here,
        # it will raise to the caller.
        foundation_info = self.load_json_file(self.params['foundations_file'])
        query_dict = self.load_json_file(self.params['queries_file'])

        try: # extract the query list from the dictionary
            queries = query_dict['queries']
        except KeyError as exn:
            self.logger.error('Error loading queries file. Missing key: %s', exn)
            return 1

        #Loop through metric/query pairs
        for qnbr, query in enumerate(queries, 1):
            self.logger.info("[%d] Starting work on query pair %s", qnbr, query)
            try:
                metric = query['metric']
                query = query['query']
            except KeyError as exn:
                self.logger.error('Error: query %d missing required key: %s',
                                  qnbr, exn)
                return 1

            # Get last datapoint from Influx and set to start time, otherwise start
            # from (datadog) beginning
            try:
                series_start = self.helper.get_metric_start_time(metric)
            except influx_help.InfluxStartQueryFailed:
                series_start = self.params['START_TIMESTAMP']
                self.logger.debug("Start time query failed, setting start time %d",
                                  series_start)

            self.send_results(series_start, metric, query, foundation_info)
            return 0


if __name__ == "__main__":
    """
    Parse command line arguments and then start the main
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--foundations-file",
                        help="The JSON foundations file")
    parser.add_argument("-i", "--influx-database",
                        help="Influx database name")
    parser.add_argument("-m", "--influx-timeout",
                        help="Influx DB request timeout")
    parser.add_argument("-p", "--influx-port",
                        help="Influx DB port number")
    parser.add_argument("-q", "--queries-file",
                        help="name of the queries file")
    parser.add_argument("-t", "--time-range",
                        help="Datadog time range (hours)")
    parser.add_argument("-u", "--influx-user",
                        help="Influx DB username")
    parser.add_argument("-w", "--influx-password",
                        help="Influx DB user password")
    parser.add_argument("-x", "--influx-host",
                        help="Influx DB host name")
    parser.add_argument("-a", "--datadog-api-key")
    parser.add_argument("-k", "--datadog-app-key")
    args = parser.parse_args()

    # set parameters from command line or environment
    params = SysParams()
    params['foundations_file'] = args.foundations_file \
                                 or constants.DEFAULT_FOUNDATIONS_JSON
    params['datadog_time_range'] = (args.time_range \
                                    or params.pop('DATADOG_TIME_RANGE')
                                   ) * constants.SEC_PER_HOUR
    params['queries_file'] = args.queries_file or params.pop('QUERIES_FILE')
    params['influx_database'] = args.influx_database or params.pop('INFLUX_DATABASE')
    params['influx_host'] = args.influx_host or params.pop('INFLUX_HOST')
    params['influx_port'] = args.influx_host or params.pop('INFLUX_PORT')
    params['influx_user'] = args.influx_user or params.pop('INFLUX_USER')
    params['influx_password'] = args.influx_password or params.pop('INFLUX_PASSWORD')
    params['influx_timeout'] = args.influx_timeout or params.pop('INFLUX_TIMEOUT')
    params['datadog_api_key'] = args.datadog_api_key or params.pop('DATADOG_API_KEY')
    params['datadog_app_key'] = args.datadog_app_key or params.pop('DATADOG_APP_KEY')

    required_env = set(['foundations_file', 'datadog_time_range', 'queries_file',
                        'influx_database', 'influx_host', 'influx_port',
                        'influx_user', 'influx_password', 'influx_timeout',
                        'datadog_api_key', 'datadog_app_key'])
    missing = required_env - set(params.keys())
    if missing:
        message = "Missing required settings: {}".format(', '.join(missing))
        Logger().logger.error(message)
        raise Exception(message)

    Exporter().run()
