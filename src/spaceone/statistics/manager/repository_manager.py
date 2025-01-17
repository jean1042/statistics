import logging

from spaceone.core.manager import BaseManager
from spaceone.statistics.error import *
from spaceone.statistics.model.storage_model import Storage
from spaceone.statistics.connector.repository_connector import RepositoryConnector

_LOGGER = logging.getLogger(__name__)


class RepositoryManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.repo_connector: RepositoryConnector = self.locator.get_connector('RepositoryConnector')

    def get_plugin(self, plugin_id, domain_id):
        return self.repo_connector.get_plugin(plugin_id, domain_id)

    def check_plugin_version(self, plugin_id, version, domain_id):
        versions = self.repo_connector.get_plugin_versions(plugin_id, domain_id)
        print(f'[PLUGIN VERSIONS] {versions}')
        if version not in versions:
            raise ERROR_INVALID_PLUGIN_VERSION(plugin_id=plugin_id, version=version)

    def deregister_plugin(self, plugin_id, domain_id):
        repository_vo = self.get_plugin(plugin_id, domain_id)
        repository_dict = repository_vo.__dict__
        if repository_dict.get('repository_id'):
            repository_vo.deregister(repository_vo['repository_id'], domain_id)

