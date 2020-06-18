import unittest
import random
from unittest.mock import patch
from datetime import datetime, timedelta
from mongoengine import connect, disconnect

from spaceone.core.unittest.result import print_data
from spaceone.core.unittest.runner import RichTestRunner
from spaceone.core import config
from spaceone.core import utils
from spaceone.core.model.mongo_model import MongoModel
from spaceone.core.transaction import Transaction
from spaceone.statistics.error import *
from spaceone.statistics.service.history_service import HistoryService
from spaceone.statistics.model.history_model import History
from spaceone.statistics.info.history_info import *
from spaceone.statistics.info.common_info import StatisticsInfo
from spaceone.statistics.connector.service_connector import ServiceConnector
from test.factory.schedule_factory import ScheduleFactory
from test.factory.history_factory import HistoryFactory


class TestHistoryService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config.init_conf(service='statistics')
        connect('test', host='mongomock://localhost')

        cls.domain_id = utils.generate_id('domain')
        cls.transaction = Transaction({
            'service': 'statistics',
            'api_class': 'History'
        })
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        disconnect()

    @patch.object(MongoModel, 'connect', return_value=None)
    def tearDown(self, *args) -> None:
        print()

    @patch.object(MongoModel, 'connect', return_value=None)
    @patch.object(ServiceConnector, 'check_resource_type', return_value=None)
    @patch.object(ServiceConnector, 'stat_resource')
    def test_create_history(self, mock_stat_resource, *args):
        new_schedule_vo = ScheduleFactory(domain_id=self.domain_id, options={
            'resource_type': 'identity.Project',
            'query': {
                'aggregate': {
                    'group': {
                        'keys': [{
                            'key': 'project_id',
                            'name': 'project_id'
                        }, {
                            'key': 'name',
                            'name': 'project_name'
                        }, {
                            'key': 'project_group.name',
                            'name': 'project_group_name'
                        }],
                    }
                },
                'sort': {
                    'name': 'resource_count',
                    'desc': True
                },
                'limit': 5
            },
            'join': [{
                'keys': ['project_id'],
                'resource_type': 'inventory.Server',
                'query': {
                    'aggregate': {
                        'group': {
                            'keys': [{
                                'key': 'project_id',
                                'name': 'project_id'
                            }],
                            'fields': [{
                                'operator': 'count',
                                'name': 'server_count'
                            }]
                        }
                    }
                }
            }, {
                'keys': ['project_id'],
                'resource_type': 'inventory.CloudService',
                'query': {
                    'aggregate': {
                        'group': {
                            'keys': [{
                                'key': 'project_id',
                                'name': 'project_id'
                            }],
                            'fields': [{
                                'operator': 'count',
                                'name': 'cloud_service_count'
                            }]
                        }
                    }
                }
            }],
            'formulas': [
                {
                    'name': 'resource_count',
                    'formula': 'server_count + cloud_service_count'
                }
            ]
        })

        mock_stat_resource.side_effect = [
            {
                'results': [{
                    'project_id': 'project-123',
                    'project_name': 'ncsoft',
                    'project_group_name': 'game'
                }, {
                    'project_id': 'project-456',
                    'project_name': 'nexon',
                    'project_group_name': 'game'
                }, {
                    'project_id': 'project-789',
                    'project_name': 'netmarble',
                    'project_group_name': 'game'
                }]
            }, {
                'results': [{
                    'project_id': 'project-123',
                    'server_count': 100
                }, {
                    'project_id': 'project-456',
                    'server_count': 65
                }, {
                    'project_id': 'project-789',
                    'server_count': 77
                }]
            }, {
                'results': [{
                    'project_id': 'project-123',
                    'cloud_service_count': 45
                }, {
                    'project_id': 'project-456',
                    'cloud_service_count': 87
                }, {
                    'project_id': 'project-789',
                    'cloud_service_count': 104
                }]
            }
        ]

        params = {
            'schedule_id': new_schedule_vo.schedule_id,
            'domain_id': self.domain_id
        }

        self.transaction.method = 'create'
        history_svc = HistoryService(transaction=self.transaction)
        result = history_svc.create(params.copy())

        self.assertIsNone(result)

    @patch.object(MongoModel, 'connect', return_value=None)
    def test_list_schedules_by_topic(self, *args):
        history_vos = HistoryFactory.build_batch(10, domain_id=self.domain_id)
        list(map(lambda vo: vo.save(), history_vos))

        params = {
            'topic': history_vos[0].topic,
            'domain_id': self.domain_id
        }

        self.transaction.method = 'list'
        history_svc = HistoryService(transaction=self.transaction)
        history_vos, total_count = history_svc.list(params.copy())
        HistoryInfo(history_vos, total_count)

        self.assertEqual(len(history_vos), 1)
        self.assertIsInstance(history_vos[0], History)
        self.assertEqual(total_count, 1)

    @patch.object(MongoModel, 'connect', return_value=None)
    def test_stat_history(self, *args):
        history_vos = HistoryFactory.build_batch(10, domain_id=self.domain_id)
        list(map(lambda vo: vo.save(), history_vos))

        params = {
            'domain_id': self.domain_id,
            'topic': history_vos[0].topic,
            'query': {
                'aggregate': {
                    'group': {
                        'keys': [{
                            'key': 'created_at',
                            'name': 'Created At'
                        }],
                        'fields': [{
                            'operator': 'count',
                            'name': 'Count'
                        }]
                    }
                },
                'sort': {
                    'name': 'Count',
                    'desc': True
                }
            }
        }

        self.transaction.method = 'stat'
        history_svc = HistoryService(transaction=self.transaction)
        values = history_svc.stat(params)
        StatisticsInfo(values)

        print_data(values, 'test_stat_history')

    @patch.object(MongoModel, 'connect', return_value=None)
    def test_stat_history_distinct(self, *args):
        history_vos = HistoryFactory.build_batch(10, domain_id=self.domain_id)
        list(map(lambda vo: vo.save(), history_vos))

        params = {
            'domain_id': self.domain_id,
            'topic': history_vos[0].topic,
            'query': {
                'distinct': 'topic'
            }
        }

        self.transaction.method = 'stat'
        history_svc = HistoryService(transaction=self.transaction)
        values = history_svc.stat(params)
        StatisticsInfo(values)

        print_data(values, 'test_stat_history_distinct')

    @patch.object(MongoModel, 'connect', return_value=None)
    def test_diff_history(self, *args):
        end = datetime.utcnow()
        start = end - timedelta(hours=24)
        point_1 = start + timedelta(hours=3)
        point_2 = start + timedelta(hours=6)
        point_3 = start + timedelta(hours=9)
        point_4 = start + timedelta(hours=12)

        self._generate_diff_history('daily_server_updates', point_1, ['BAREMETAL'])
        self._generate_diff_history('daily_server_updates', point_2)
        self._generate_diff_history('daily_server_updates', point_3)
        self._generate_diff_history('daily_server_updates', point_4, ['HYPERVISOR'])

        params = {
            'topic': 'daily_server_updates',
            'from': 'now - 24h',
            'default_fields': ['server_type', 'provider'],
            'diff_fields': ['server_count', 'total_cpu'],
            'domain_id': self.domain_id,
        }

        self.transaction.method = 'diff'
        history_svc = HistoryService(transaction=self.transaction)
        values = history_svc.diff(params)
        StatisticsInfo(values)

        print_data(values, 'test_diff_history')

    def _generate_diff_history(self, topic, created_at, remove_server_type=[]):
        if 'VM' not in remove_server_type:
            HistoryFactory(topic=topic, created_at=created_at, domain_id=self.domain_id, values={
                'server_type': 'VM',
                'provider': 'openstack',
                'server_count': float(random.randrange(0, 100)),
                'total_cpu': float(random.randrange(0, 1000))
            })

        if 'BAREMETAL' not in remove_server_type:
            HistoryFactory(topic=topic, created_at=created_at, domain_id=self.domain_id, values={
                'server_type': 'BAREMETAL',
                'provider': 'openstack',
                'server_count': float(random.randrange(0, 100)),
                'total_cpu': float(random.randrange(0, 1000))
            })

        if 'HYPERVISOR' not in remove_server_type:
            HistoryFactory(topic=topic, created_at=created_at, domain_id=self.domain_id, values={
                'server_type': 'HYPERVISOR',
                'provider': 'openstack',
                'server_count': float(random.randrange(0, 100)),
                'total_cpu': float(random.randrange(0, 1000))
            })


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)
