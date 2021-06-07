import unittest
import copy
import os
from unittest.mock import patch
from mongoengine import connect, disconnect
from google.protobuf.json_format import MessageToDict
from google.protobuf.empty_pb2 import Empty

from spaceone.core.unittest.result import print_message
from spaceone.core.unittest.runner import RichTestRunner
from spaceone.core import config
from spaceone.core import utils
from spaceone.core.service import BaseService
from spaceone.core.locator import Locator
from spaceone.core.pygrpc import BaseAPI
from spaceone.api.statistics.v1 import storage_pb2
from spaceone.statistics.api.v1.storage import Storage
from test.factory.storage_factory import StorageFactory
from spaceone.statistics.connector import PluginConnector
from spaceone.core.model.mongo_model import MongoModel


class _MockStorageService(BaseService):
    '''
    def add(self, params):
        params = copy.deepcopy(params)
        if 'tags' in params:
            params['tags'] = utils.dict_to_tags(params['tags'])

        return ScheduleFactory(**params)

    def update(self, params):
        params = copy.deepcopy(params)
        if 'tags' in params:
            params['tags'] = utils.dict_to_tags(params['tags'])

        return ScheduleFactory(**params)

    def delete(self, params):
        pass

    def enable(self, params):
        return ScheduleFactory(**params)

    def disable(self, params):
        return ScheduleFactory(**params)

    def get(self, params):
        return ScheduleFactory(**params)

    def list(self, params):
        return ScheduleFactory.build_batch(10, **params), 10

    def stat(self, params):
        return {
            'results': [{'project_id': utils.generate_id('project'), 'server_count': 100}]
        }
    '''

    def get(self, params):
        params = copy.deepcopy(params)
        return StorageFactory(**params)

    def register(self, params):
        return StorageFactory(**params)

    def update(self, params):
        params = copy.deepcopy(params)
        return StorageFactory(**params)

    def list(self, params):
        return StorageFactory.build_batch(10, **params), 10

    def enable(self, params):
        return StorageFactory(**params)

    def disable(self, params):
        return StorageFactory(**params)

    def deregister(self, params):
        return StorageFactory(**params)

    def update_plugin(self, params):
        return StorageFactory(**params)

    def verify_plugin(self, params):
        return StorageFactory(**params)

class TestStorageAPI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config.init_conf(package='spaceone.statistics')
        connect('test', host='mongomock://localhost')
        config_path = os.environ.get('SPACEONE_CONFIG_FILE')

        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        disconnect()

    @patch.object(BaseAPI, '__init__', return_value=None)
    @patch.object(Locator, 'get_service', return_value=_MockStorageService())
    @patch.object(BaseAPI, 'parse_request')
    def test_register_storage(self, mock_parse_request, *args):
        params = {
            'name': utils.generate_id('storage', 4),
            'tags': {
                utils.random_string(5): utils.random_string(5)
            },
            'plugin_info': {
                'plugin_id': utils.generate_id('plugin'),
                'version': '1.1',
                'secret_id': utils.generate_id('secret')
            },
            'user_id': utils.generate_id('user'),
            'domain_id': utils.generate_id('domain')
        }
        mock_parse_request.return_value = (params, {})
        storage_servicer = Storage()

        storage_info = storage_servicer.register(params, {})
        print_message(storage_info, 'test_register_storage')

        storage_data = MessageToDict(storage_info, preserving_proto_field_name=True)

        self.assertIsInstance(storage_info, storage_pb2.StorageInfo)
        self.assertEqual(storage_info.name, params['name'])
        self.assertEqual(storage_data['state'], 'ENABLED')
        # self.assertIsNotNone(storage_info.capability)
        self.assertDictEqual(storage_data['tags'], params['tags'])
        self.assertIsInstance(storage_info.plugin_info, storage_pb2.PluginInfo)  # Check if 'PluginInfo' exists
        self.assertEqual(storage_data['plugin_info']['plugin_id'], params['plugin_info']['plugin_id'])
        self.assertEqual(storage_data['plugin_info']['version'], params['plugin_info']['version'])
        self.assertEqual(storage_data['domain_id'], params['domain_id'])
        self.assertIsNotNone(getattr(storage_info, 'created_at', None))

        print(f'[TEST REGISTER STORAGE] {storage_data}')

    @patch.object(BaseAPI, '__init__', return_value=None)
    @patch.object(Locator, 'get_service', return_value=_MockStorageService())
    @patch.object(BaseAPI, 'parse_request')
    def test_update_storage(self, mock_parse_request, *args):
        params = {
            'storage_id': utils.generate_id('storage'),
            'name': 'update-storage-name',
            'tags': {
                'update_key': 'update_value'
            },
            'domain_id': utils.generate_id('domain')
        }
        mock_parse_request.return_value = (params, {})

        storage_servicer = Storage()
        storage_info = storage_servicer.update(params, {})

        print_message(storage_info, 'test_update_schedule')
        storage_data = MessageToDict(storage_info, preserving_proto_field_name=True)

        self.assertIsInstance(storage_info, storage_pb2.StorageInfo)
        self.assertEqual(storage_data['name'], params['name'])
        self.assertEqual(storage_data['storage_id'], params['storage_id'])
        self.assertDictEqual(storage_data['tags'], params['tags'])

        print(f'[TEST UPDATE STORAGE] {storage_data}')

    @patch.object(BaseAPI, '__init__', return_value=None)
    @patch.object(Locator, 'get_service', return_value=_MockStorageService())
    @patch.object(BaseAPI, 'parse_request')
    def test_get_storage(self, mock_parse_request, *args):
        mock_parse_request.return_value = ({}, {})
        params = {
            'domain_id': utils.generate_id('domain'),
            'storage_id': utils.generate_id('storage')
        }
        storage_servicer = Storage()
        storage_info = storage_servicer.get(params, {})
        storage_data = MessageToDict(storage_info, preserving_proto_field_name=True)

        print_message(storage_info, 'test_get_schedule')
        self.assertIsInstance(storage_info, storage_pb2.StorageInfo)

        print(f'[TEST GET STORAGE] {storage_data}')

    @patch.object(BaseAPI, '__init__', return_value=None)
    @patch.object(Locator, 'get_service', return_value=_MockStorageService())
    @patch.object(BaseAPI, 'parse_request')
    def test_list_schedules(self, mock_parse_request, *args):
        mock_parse_request.return_value = ({}, {})

        storage_servicer = Storage()
        schedules_info = storage_servicer.list({}, {})

        print_message(schedules_info, 'test_list_schedules')

        self.assertIsInstance(schedules_info, storage_pb2.StoragesInfo)
        self.assertIsInstance(schedules_info.results[0], storage_pb2.StorageInfo)
        self.assertEqual(schedules_info.total_count, 10)

    @patch.object(BaseAPI, '__init__', return_value=None)
    @patch.object(Locator, 'get_service', return_value=_MockStorageService())
    @patch.object(BaseAPI, 'parse_request')
    def test_enable_storage(self, mock_parse_request, *args):
        params = {
            'storage_id': utils.generate_id('storage'),
            'state': 'ENABLED',
            'domain_id': utils.generate_id('domain')
        }
        mock_parse_request.return_value = (params, {})

        storage_servicer = Storage()
        storage_info = storage_servicer.enable(params, {})
        storage_data = MessageToDict(storage_info, preserving_proto_field_name=True)

        print_message(storage_info, 'test_enable_storage')

        self.assertIsInstance(storage_info, storage_pb2.StorageInfo)
        self.assertEqual(storage_info.state, storage_pb2.StorageInfo.State.ENABLED)

        print(f'[TEST ENABLE STORAGE] {storage_data}')

    @patch.object(BaseAPI, '__init__', return_value=None)
    @patch.object(Locator, 'get_service', return_value=_MockStorageService())
    @patch.object(BaseAPI, 'parse_request')
    def test_disable_storage(self, mock_parse_request, *args):
        params = {
            'storage_id': utils.generate_id('storage'),
            'state': 'DISABLED',
            'domain_id': utils.generate_id('domain')
        }
        mock_parse_request.return_value = (params, {})

        storage_servicer = Storage()
        storage_info = storage_servicer.disable(params, {})
        storage_data = MessageToDict(storage_info, preserving_proto_field_name=True)

        print_message(storage_info, 'test_disable_storage')

        self.assertIsInstance(storage_info, storage_pb2.StorageInfo)
        self.assertEqual(storage_info.state, storage_pb2.StorageInfo.State.DISABLED)

        print(f'[TEST DISABLE STORAGE] {storage_data}')

    @patch.object(BaseAPI, '__init__', return_value=None)
    @patch.object(Locator, 'get_service', return_value=_MockStorageService())
    @patch.object(BaseAPI, 'parse_request')
    def test_deregister_storage(self, mock_parse_request, *args):
        params = {
            'storage_id': utils.generate_id('storage'),
            'domain_id': utils.generate_id('domain')
        }
        mock_parse_request.return_value = (params, {})

        storage_servicer = Storage()
        storage_info = storage_servicer.deregister(params, {})
        storage_data = MessageToDict(storage_info, preserving_proto_field_name=True)

        print_message(storage_info, 'test_deregister_storage')
        # TODO : ASK!!
        # self.assertIsInstance(storage_info, Empty)
        # self.assertEqual(storage_info.state, storage_pb2.StorageInfo.State.DISABLED)

        print(f'[TEST DEREGISTER STORAGE] {storage_data}')

    @patch.object(BaseAPI, '__init__', return_value=None)
    @patch.object(Locator, 'get_service', return_value=_MockStorageService())
    @patch.object(PluginConnector, '__init__', return_value=None)
    @patch.object(PluginConnector, 'initialize', return_value='grpc://plugin.spaceone.dev:50051')
    @patch.object(PluginConnector, 'get_plugin_endpoint', return_value='grpc://plugin.spaceone.dev:50051')
    @patch.object(BaseAPI, 'parse_request')
    def test_update_plugin(self, mock_parse_request, *args):
        params = {
            'storage_id': utils.generate_id('storage'),
            'name': 'storage-plugin-update',
            'plugin_info': {
                'plugin_id': utils.generate_id('storage'),
                'version': '3.0',
                'options': {},
            },
            'tags': {
                'update_key': 'update_value'
            },
            'domain_id': utils.generate_id('domain')
        }
        mock_parse_request.return_value = (params, {})
        storage_servicer = Storage()
        storage_info = storage_servicer.update_plugin(params, {})
        storage_data = MessageToDict(storage_info, preserving_proto_field_name=True)
        print_message(storage_info, 'test_update_storage_plugin')

        self.assertIsInstance(storage_info, storage_pb2.StorageInfo)
        self.assertEqual(storage_info.name, params['name'])
        self.assertDictEqual(storage_data['tags'], params['tags'])
        self.assertEqual(storage_info.plugin_info.version, params['plugin_info']['version'])
        self.assertIsNotNone(storage_info.plugin_info)

        print(f'[TEST UPDATE STORAGE PLUGIN] {storage_data}')

    @patch.object(BaseAPI, '__init__', return_value=None)
    @patch.object(Locator, 'get_service', return_value=_MockStorageService())
    @patch.object(PluginConnector, '__init__', return_value=None)
    @patch.object(PluginConnector, 'initialize', return_value='grpc://plugin.spaceone.dev:50051')
    @patch.object(PluginConnector, 'get_plugin_endpoint', return_value='grpc://plugin.spaceone.dev:50051')
    @patch.object(BaseAPI, 'parse_request')
    def test_verify_plugin(self, mock_parse_request, *args):
        params = {
            'storage_id': utils.generate_id('storage'),
            'domain_id': utils.generate_id('domain')
        }
        mock_parse_request.return_value = (params, {})
        storage_servicer = Storage()
        storage_info = storage_servicer.verify_plugin(params, {})
        storage_data = MessageToDict(storage_info, preserving_proto_field_name=True)
        print_message(storage_info, 'test_deregister_storage_plugin')

        self.assertIsInstance(storage_info, Empty)

        print(f'[TEST VERIFY STORAGE PLUGIN] {storage_data}')

if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)
