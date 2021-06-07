import logging

from spaceone.core.manager import BaseManager
from spaceone.core import utils
from spaceone.statistics.error import *
from spaceone.statistics.model.storage_model import Storage
from spaceone.statistics.connector.plugin_connector import PluginConnector
from spaceone.statistics.connector.repository_connector import RepositoryConnector
_LOGGER = logging.getLogger(__name__)


class PluginManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.plugin_connector: PluginConnector = self.locator.get_connector('PluginConnector')
        self.repository_connector: RepositoryConnector = self.locator.get_connector('RepositoryConnector')

    def initialize(self, plugin_id, version, domain_id):
        endpoint = self.plugin_connector.get_plugin_endpoint(plugin_id, version, domain_id)
        _LOGGER.debug(f'[init_plugin] endpoint: {endpoint}')
        self.plugin_connector.initialize(endpoint)

    def init_plugin(self, options):
        plugin_info = self.plugin_connector.init(options)
        _LOGGER.debug(f'[plugin_info] {plugin_info}')
        plugin_metadata = plugin_info.get('metadata', {})

        self._validate_plugin_metadata(plugin_metadata)
        return plugin_metadata

    def register_plugin(self, plugin_info, domain_id):
        name = utils.generate_id('storage', 4)
        image = ''
        plugin_info = self.repository_connector.register_plugin(name, image, domain_id)
        _LOGGER.debug(f'[plugin_info]{plugin_info}')
        plugin_metadata = plugin_info.get('metadata', {})

        self._validate_plugin_metadata(plugin_metadata)
        print(f'[PLUGIN_INFO AFTER INIT] {plugin_info}')
        return self.update_storage_by_vo(params=plugin_info)

    def update_storage_by_vo(self, params, storage_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[update_storage_by_vo._rollback] Revert Data : '
                         f'{old_data["storage_id"]}')
            storage_vo.update(old_data)

        self.transaction.add_rollback(_rollback, storage_vo.to_dict())

        return storage_vo.update(params)

    def list_domains(self, query):
        identity_connector = self.locator.get_connector('IdentityConnector')
        return identity_connector.list_domains(query)

    def verify_plugin(self, params, secret_data):
        self.plugin_connector.verify(params, secret_data)

    @staticmethod
    def _validate_plugin_metadata(plugin_metadata):
        try:
            plugin_metadata.validate()
        except Exception as e:
            raise ERROR_INVALID_PLUGIN_OPTIONS(reason=str(e))
