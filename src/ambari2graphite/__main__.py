import sys
import argparse
import configparser

import os
import json

import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta

import graphyte

from .exc import DataCollectionError
from .settings import METRICS

BASE_TIMESTAMP_TO_UPLOAD = 1573988160


def batchify(iterable, batch_length=1):
    length = len(iterable)
    for index in range(0, length, batch_length):
        yield iterable[index:min(index + batch_length, length)]


class Ambari2Graphite:
    def __init__(self, config, test_start_date):
        self.config = config
        self.settings = {}
        self.data = []
        self.test_start_date = None
        if test_start_date:
            self.test_start_timestamp = int(test_start_date.strftime("%s")) - 60

    @property
    def output_file_path(self):
        return self.settings.get('output_file_path') \
            or self.config['ambari']['cluster_name']

    @property
    def cluster_label(self):
        return self.settings.get('cluster_label') \
            or self.config['ambari']['cluster_name']

    def get_full_url(self, module):
        try:
            return 'https://{subdomain}.azurehdinsight.net/api/v1/clusters/{cluster_name}/{module}'.format(  # NOQA
                subdomain=self.config['ambari']['subdomain'],
                cluster_name=self.config['ambari']['cluster_name'],
                module=module
            )
        except KeyError as e:
            raise DataCollectionError(e)

    def collect_results_from_ambari(self, start_date=None, save_data=False):
        param_str = ''

        if start_date:
            end_date = datetime.now()
            period = 15
            param_str = '[{start_date},{end_date},{period}]'.format(
                start_date=start_date.strftime("%s"),
                end_date=end_date.strftime("%s"),
                period=period
            )
        for package_idx, package in enumerate(METRICS):
            for idx, batch in enumerate(batchify([i for i in package['metrics']], 10)):
                params = package.get('params', {})
                params['fields'] = ','.join(
                    [
                        '{}._avg{}'.format(
                            i.path,
                            param_str
                        )
                        for i in batch
                    ]
                )
                auth = HTTPBasicAuth(
                    self.config['ambari']['username'],
                    self.config['ambari']['password']
                )
                url = self.get_full_url(package['module'])
                response = requests.get(url, auth=auth, params=params)
                response.raise_for_status()
                # if response.status_code != 200:
                #     raise DataCollectionError("Cannot load data from ambari")

                try:
                    response_data = response.json()
                    if save_data:
                        self.data.append(response_data)
                except ValueError as e:
                    raise DataCollectionError(
                        "Error during parsing JSON. {}".format(e.message)
                    )

                with open(
                    '{}___{}___{}'.format(
                        self.output_file_path,
                        package_idx,
                        idx
                    ),
                    'w'
                ) as f:
                    f.write(response.text)

    def get_statistics_from_folder(self, path):
        file_list = os.listdir(path)
        # TODO: walk?
        for file in file_list:
            file_path = os.path.join(path, file)
            if os.path.isdir(file_path):
                continue
            self.get_statistics_from_file(file_path)
            self.send_statistics()
            # for file in os.listdir(path):

    def get_statistics_from_file(self, path):
        with open(path, 'r') as f:
            try:
                self.data = json.loads(f.read())
            except ValueError as e:
                print(e)
                print('Error parsing JSON: {}'.format(path))

    def send_statistics(self):
        graphite_url = self.config['graphite']['url']
        prefix = self.config['graphite']['base_prefix'].format(
            self.cluster_label
        )
        graphyte.init(graphite_url, prefix=prefix)
        metric_path = ''
        service_component_info = self.data.get('ServiceComponentInfo')
        if service_component_info:
            metric_path = '{service}.{component}Metrics'.format(
                service=service_component_info.get('service_name'),
                component=service_component_info.get('component_name')
            )
            metrics = self.data.get('metrics')
            if not metrics:
                return
            self.recursive_parse_metric(metric_path, metrics)
        else:
            items = self.data.get('items')
            if items:
                for item in items:
                    host_info = item.get('HostRoles')
                    host_name = host_info.get('host_name') \
                        .split('.')[0].split('-')[0]
                    metric_path = '{host_name}.{component}Metrics'.format(
                        host_name=host_name,
                        component=host_info.get('component_name')
                    )
                    metrics = item.get('metrics')
                    if not metrics:
                        return
                    self.recursive_parse_metric(metric_path, metrics)
            else:
                raise NotImplementedError('Incorrect format')

    def recursive_parse_metric(self, metric_path, data):
        if isinstance(data, dict):
            for k, v in data.items():
                name = '{}.{}'.format(metric_path, k)
                self.recursive_parse_metric(name, v)
        if isinstance(data, list):
            for value, timestamp in data:
                if not (
                    self.test_start_timestamp and
                    timestamp < self.test_start_timestamp
                ):
                    delta = timestamp - self.test_start_timestamp
                    relative_timestamp = BASE_TIMESTAMP_TO_UPLOAD + delta
                    graphyte.send(metric_path, value, relative_timestamp)


def main(argv=sys.argv):
    description = """"""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        'config_path',
        metavar='config_path',
        help='Config file path'
    )

    parser.add_argument(
        '-s'
        '--start-date',
        dest='start_date',
        help='Start date in format (YYYY-MM-DD hh:mm:ss)'
    )

    parser.add_argument(
        '-t',
        '--test-start-date',
        dest='test_start_date',
        help='Test start date in format (YYYY-MM-DD hh:mm:ss)'
    )

    parser.add_argument(
        '-l',
        dest='use_local_data',
        action='store_true',
        default=False,
        help='Use data from requests instead files'
    )

    parser.add_argument(
        '-i'
        '--input-path',
        dest='input_path',
        help='Input files path.'
    )

    args = parser.parse_args(argv[1:])

    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d %H:%M:%S')
    else:
        start_date = 0

    test_start_date = None
    if args.test_start_date:
        test_start_date = datetime.strptime(
            args.test_start_date,
            '%Y-%m-%d %H:%M:%S'
        )

    config = configparser.ConfigParser()
    config.read(args.config_path)

    processor = Ambari2Graphite(config, test_start_date)
    # processor.collect_results_from_ambari(start_date, args.use_local_data)

    processor.get_statistics_from_folder(args.input_path)


if __name__ == '__main__':
    main()
