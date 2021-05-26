import logging

from spaceone.core.manager import BaseManager
from spaceone.statistics.error import *
from spaceone.statistics.model.storage_model import Storage
from spaceone.statistics.connector.plugin_connector import PluginConnector
_LOGGER = logging.getLogger(__name__)


class PluginManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.plugin_connector: PluginConnector = self.locator.get_connector('PluginConnector')

    def initialize(self, plugin_id, version, domain_id):
        endpoint = self.plugin_connector.get_plugin_endpoint(plugin_id, version, domain_id)
        _LOGGER.debug(f'[init_plugin] endpoint: {endpoint}')
        self.plugin_connector.initilize(endpoint)

    def init_plugin(self, options):
        plugin_info = self.plugin_connector.init(options)
        _LOGGER.debug(f'[plugin_info]{plugin_info}')
        plugin_metadata = plugin_info.get('metadata', {})

        self._validate_plugin_metadata(plugin_metadata, monitoring_type)
        return self.update_storage_by_vo()

    def update_storage_plugin(self, params):
        storage_vo: Storage = self.get_storage(params['storage_id'], params['domain_id'])
        return self.update_storage_by_vo(params, storage_vo)

    def update_storage_by_vo(self, params, storage_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[update_storage_by_vo._rollback] Revert Data : '
                         f'{old_data["storage_id"]}')
            storage_vo.update(old_data)

        self.transaction.add_rollback(_rollback, storage_vo.to_dict())

        return storage_vo.update(params)

    def deregister_storage(self, storage_id, domain_id):
        storage_vo: Storage = self.get_storage(storage_id, domain_id)
        storage_vo.deregister()

    def get_storage(self, storage_id, domain_id, only=None):
        return self.storage_model.get(storage_id=storage_id, domain_id=domain_id, only=only)

    def list_storages(self, query={}):
        return self.storage_model.query(**query)

    def stat_storages(self, query):
        return self.storage_model.stat(**query)

    def list_domains(self, query):
        identity_connector = self.locator.get_connector('IdentityConnector')
        return identity_connector.list_domains(query)

    @staticmethod
    def _validate_plugin_metadata(plugin_metadata):
        try:
            plugin_metadata.validate()
        except Exception as e:
            raise ERROR_INVALID_PLUGIN_OPTIONS(reason=str(e))
