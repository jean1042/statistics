import logging

from spaceone.core.manager import BaseManager
from spaceone.statistics.error import *
from spaceone.statistics.model.storage_model import Storage
_LOGGER = logging.getLogger(__name__)


class StorageManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage_model: Storage = self.locator.get_model('Storage')

    def register_storage(self, params):
        def _rollback(storage_vo):
            _LOGGER.info(f'[register_storage._rollback] '
                         f'Delete storage : {storage_vo.name} '
                         f'({storage_vo.schedule_id})')
            storage_vo.deregister()

        storage_vo: Storage = self.storage_model.create(params)
        self.transaction.add_rollback(_rollback, storage_vo)

        return storage_vo

    def update_storage(self, params):
        storage_vo: Storage = self.get_storage(params['storage_id'], params['domain_id'])
        return self.update_storage_by_vo(params, storage_vo)

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
