import logging
import copy

from spaceone.core.service import *
from spaceone.core import utils

from spaceone.statistics.error import *
from spaceone.statistics.manager.resource_manager import ResourceManager
from spaceone.statistics.manager.schedule_manager import ScheduleManager
from spaceone.statistics.manager.storage_manager import StorageManager
from spaceone.statistics.manager.plugin_manager import PluginManager
from spaceone.statistics.manager.secret_manager import SecretManager
from spaceone.statistics.manager.repository_manager import RepositoryManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class StorageService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.resource_mgr: ResourceManager = self.locator.get_manager('ResourceManager')
        self.schedule_mgr: ScheduleManager = self.locator.get_manager('ScheduleManager')
        self.storage_mgr: StorageManager = self.locator.get_manager('StorageManager')
        self.plugin_mgr: PluginManager = self.locator.get_manager('PluginManager')
        self.repository_mgr: RepositoryManager = self.locator.get_manager('RepositoryManager')

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['name', 'domain_id', 'user_id', 'plugin_info'])
    def register(self, params):
        """Register storage for statistics

        Args:
            params (dict): {
                'name': 'str',
                'plugin_info': 'dict',
                'tags': 'dict',
                'domain_id': 'str',
                'user_id': 'str'
            }

        Returns:
            storage_vo
        """

        domain_id = params['domain_id']
        _plugin_info = copy.deepcopy(params['plugin_info'])
        print(f'[SERVICE] Params {params}')
        params['state'] = 'ENABLED'

        if 'secret_data' in _plugin_info:
            del _plugin_info['secret_data']

        self._check_plugin_info(params['plugin_info'])

        # Update metadata
        plugin_metadata = self._init_plugin(params['plugin_info'], domain_id)
        params['plugin_info']['metadata'] = plugin_metadata
        new_secret = self._create_secret(params['plugin_info'], domain_id)

        _plugin_info['secret_id'] = new_secret.get('secret_id')
        # _plugin_info['options'] = {}
        # _plugin_info['metadata'] = plugin_metadata
        params['plugin_info'] = _plugin_info
        # params['capability'] = {}
        return self.storage_mgr.register_storage(params)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['storage_id', 'domain_id'])
    def update(self, params):
        """Update storage

        Args:
            params (dict): {
                'storage_id': 'str',
                'name': 'str'
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            storage_vo
        """
        return self.storage_mgr.update_storage(params)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['storage_id', 'domain_id'])
    def update_plugin(self, params):
        """Update plugin

        Args:
            params (dict): {
                'storage_id': 'str',
                'version': 'str',
                'options': 'dict',
                'domain_id': 'str',
            }

        Returns:
            storage_vo (object)

        NOTE:
        plugin_info = {
            plugin_id, version, [options(dict)], [secret_data(dict)], [schema]
        }
        """
        storage_id = params['storage_id']
        domain_id = params['domain_id']
        version = params.get('version')
        options = params.get('options')

        storage_vo = self.storage_mgr.get_storage(storage_id, domain_id)
        storage_dict = storage_vo.to_dict()
        plugin_info = storage_dict['plugin_info']

        # Update plugin_version and options
        if version:
            domain_id = params['domain_id']
            plugin_info['version'] = version
            metadata = self._init_plugin(plugin_info=plugin_info, domain_id=domain_id)
            plugin_info['metadata'] = metadata

        if options or options == {}:
            plugin_info['options'] = options  # Overwrite

        params = {
            'plugin_id': plugin_info['plugin_id'],
            'domain_id': domain_id,
            'plugin_info': plugin_info
        }

        _LOGGER.debug(f'[update_plugin] {plugin_info}')

        return self.storage_mgr.update_storage_by_vo(params, storage_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['storage_id', 'domain_id'])
    def verify_plugin(self, params):
        """ Verify storage plugin
        Args:
            params (dict): {
                'storage_id': 'str',
                'domain_id': 'str'
            }
        Returns:
            storage_vo (object)
        """

        storage_id = params['storage_id']
        domain_id = params['domain_id']
        storage_vo = self.storage_mgr.get_storage(storage_id, domain_id)

        self._verify_plugin(storage_vo.plugin_info, storage_vo.capability, domain_id)

        # return {'status': True}

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['storage_id', 'domain_id'])
    def enable(self, params):
        """Enable storage

        Args:
            params (dict): {
                'storage_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            schedule_vo
        """

        domain_id = params['domain_id']
        storage_id = params['storage_id']

        storage_vo = self.storage_mgr.get_storage(storage_id, domain_id)
        return self.storage_mgr.update_storage_by_vo({'state': 'ENABLED'}, storage_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['storage_id', 'domain_id'])
    def disable(self, params):
        """Disable storage

        Args:
            params (dict): {
                'storage_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            storage_vo
        """

        domain_id = params['domain_id']
        storage_id = params['storage_id']

        storage_vo = self.storage_mgr.get_storage(storage_id, domain_id)
        return self.storage_mgr.update_storage_by_vo({'state': 'DISABLED'}, storage_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['storage_id', 'domain_id'])
    def deregister(self, params):
        """Deregister storage

        Args:
            params (dict): {
                'storage_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """
        # self.repository_mgr.deregister_storage(params['storage_id'], params['domain_id'])
        storage_vo = self.storage_mgr.get_storage(params['storage_id'], params['domain_id'])
        # return self.storage_mgr.delete()
        return self.storage_mgr.update_storage_by_vo({'state': 'DISABLED'}, storage_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['storage_id', 'domain_id'])
    def get(self, params):
        """Get storage

        Args:
            params (dict): {
                'storage_id': 'str',
                'domain_id': 'str',
                'only': 'list'
            }

        Returns:
            storage_vo
        """

        return self.storage_mgr.get_storage(params['storage_id'], params['domain_id'], params.get('only'))

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['domain_id'])
    @append_query_filter(['storage_id', 'name', 'state', 'user_id', 'domain_id'])
    @change_tag_filter('tags')
    @append_keyword_filter(['storage_id'])
    def list(self, params):
        """ List storages

        Args:
            params (dict): {
                'schedule_id': 'str',
                'name' : 'str',
                'state': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)'
            }

        Returns:
            storage_vos (object)
            total_count
        """
        query = params.get('query', {})
        return self.storage_mgr.list_storages(query)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @change_tag_filter('tags')
    @append_keyword_filter(['storage_id'])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get('query', {})
        return self.storage_mgr.stat_storages(query)

    def _init_plugin(self, plugin_info, domain_id):
        plugin_id = plugin_info['plugin_id']
        version = plugin_info.get('version', '')
        options = plugin_info.get('options', '')

        plugin_mgr: PluginManager = self.locator.get_manager('PluginManager')
        print(f'f[PLUGIN ID: {plugin_id}]')
        plugin_mgr.initialize(plugin_id, version, domain_id)  # Get plugin's new endpoint
        return plugin_mgr.init_plugin(plugin_info, domain_id)

    @staticmethod
    def _check_plugin_info(plugin_info):
        if 'plugin_id' not in plugin_info:
            raise ERROR_REQUIRED_PARAMETER(key='plugin_info.plugin_id')

        if 'version' not in plugin_info:
            raise ERROR_REQUIRED_PARAMETER(key='plugin_info.version')

        secret_data = plugin_info.get('secret_data')
        if secret_data is None:
            raise ERROR_REQUIRED_PARAMETER(key='plugin_info.secret_data')

    def _check_secret_info(self, secret_data):
        if 'name' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.name')

        if 'data' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.data')

    def _create_secret(self, plugin_info, domain_id):
        secret_data = plugin_info.get('secret_data')
        self._check_secret_info(secret_data)
        secret_name = utils.generate_id('secret', 4)
        secret_mgr: SecretManager = self.locator.get_manager('SecretManager')
        new_secret = secret_mgr.create_secret(name=secret_name, data=secret_data['data'], secret_type='CREDENTIALS', domain_id=domain_id)
        return new_secret

    def _verify_plugin(self, plugin_info, capability, domain_id):
        plugin_id = plugin_info['plugin_id']
        version = plugin_info['version']
        options = plugin_info['options']
        secret_id = plugin_info.get('secret_id')

        secret_mgr: SecretManager = self.locator.get_manager('SecretManager')
        secret_data, schema = secret_mgr.get_plugin_secret_data(secret_id, domain_id)

        plugin_mgr: PluginManager = self.locator.get_manager('PluginManager')
        plugin_mgr.initialize(plugin_id, version, domain_id)
        plugin_mgr.verify_plugin(options, secret_data, schema)


