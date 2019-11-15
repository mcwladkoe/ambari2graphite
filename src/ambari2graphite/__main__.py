import sys
import argparse
import configparser

import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta

import graphyte

from .exc import DataCollectionError
from .settings import YARN_ACTIVE_RES_MANAGERS


def batchify(iterable, batch_length=1):
    length = len(iterable)
    for index in range(0, length, batch_length):
        yield iterable[index:min(index + batch_length, length)]


class Ambari2Graphite:
    def __init__(self, config):
        self.config = config
        self.settings = {}

    @property
    def output_file_path(self):
        return self.settings.get('output_file_path') \
            or self.config['ambari']['cluster_name']

    @property
    def cluster_label(self):
        return self.settings.get('cluster_label') \
            or self.config['ambari']['cluster_name']

    def get_full_url(self):
        try:
            return 'https://{subdomain}.azurehdinsight.net/api/v1/clusters/{cluster_name}'.format(  # NOQA
                subdomain=self.config['ambari']['subdomain'],
                cluster_name=self.config['ambari']['cluster_name']
            )
        except KeyError as e:
            raise DataCollectionError(e)

    def collect_results_from_ambari(self):
        url = self.get_full_url()
        for idx, batch in enumerate(batchify(YARN_ACTIVE_RES_MANAGERS, 10)):
            params = {
                'fields': ','.join(
                    ['{}._avg'.format(i) for i in batch]
                )
            }
            # 'metrics/load/1-min._avg[1573093309,1573096909,15]'
            auth = HTTPBasicAuth(
                self.config['ambari']['username'],
                self.config['ambari']['password']
            )
            response = requests.get(url, auth=auth, params=params)
            response.raise_for_status()
            # if response.status_code != 200:
            #     raise DataCollectionError("Cannot load data from ambari")

            try:
                self.data = response.json()
            except ValueError as e:
                raise DataCollectionError(
                    "Error during parsing JSON. {}".format(e.message)
                )

            with open('{}___{}'.format(self.output_file_path, idx), 'w') as f:
                f.write(response.text)

    def send_statistics(self):
        graphite_url = self.config['graphite']['url']
        prefix = self.config['graphite']['base_prefix'].format(
            self.cluster_label
        )
        graphyte.init(graphite_url, prefix=prefix)
        metrics = self.data.get('metrics')
        if not metrics:
            return
        for group, group_data in metrics.items():
            for metric, metric_data in group_data.items():
                for val, timestamp in metric_data:
                    graphyte.send(metric, val, timestamp)


def main(argv=sys.argv):
    description = """"""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        'config_path',
        metavar='config_path',
        help='Config file path'
    )

    args = parser.parse_args(argv[1:])

    config = configparser.ConfigParser()
    config.read(args.config_path)

    processor = Ambari2Graphite(config)
    processor.collect_results_from_ambari()

    # collect_results(config)
    # send_statistics(config)x


if __name__ == '__main__':
    main()
