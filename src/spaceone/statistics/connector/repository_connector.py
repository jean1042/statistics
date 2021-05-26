import logging

from google.protobuf.json_format import MessageToDict

from spaceone.core.connector import BaseConnector
from spaceone.core import pygrpc
from spaceone.core.utils import parse_endpoint
from spaceone.core.error import *
from spaceone.statistics.error.resource import *

__all__ = ['RepositoryConnector']

_LOGGER = logging.getLogger(__name__)


class RepositoryConnector(BaseConnector):

    def __init__(self, transaction, config):
        super().__init__(transaction, config)
        self.client = {}

    def _init_client(self, service, resource):
        if service not in self.client:
            if service not in self.config:
                raise ERROR_INVALID_RESOURCE_TYPE(resource_type=f'{service}.{resource}')

            e = parse_endpoint(self.config[service])
            if e.get('path') is None:
                raise ERROR_CONNECTOR_CONFIGURATION(backend=self.__class__.__name__)

            self.client[service] = pygrpc.client(endpoint=f'{e.get("hostname")}:{e.get("port")}')

    def _check_config(self, service, resource):
        if service not in self.client.keys():
            raise ERROR_NOT_SUPPORT_RESOURCE_TYPE(resource_type=f'{service}.{resource}')

        if not hasattr(self.client[service], resource):
            raise ERROR_NOT_SUPPORT_RESOURCE_TYPE(resource_type=f'{service}.{resource}')

        if not hasattr(getattr(self.client[service], resource), 'stat'):
            raise ERROR_NOT_SUPPORT_RESOURCE_TYPE(resource_type=f'{service}.{resource}')

    def get_plugin(self, service, resource, query, domain_id):
        _LOGGER.debug(f'[stat_resource] {service}.{resource} : {query}')

        self._init_client(service, resource)
        self._check_config(service, resource)

        response = getattr(self.client[service], resource).stat({
            'domain_id': domain_id,
            'query': query
        }, metadata=self.transaction.get_connection_meta())

        return self._change_message(response)

    def get_plugin_versions(self, service, resource, query, domain_id):
        _LOGGER.debug(f'[stat_resource] {service}.{resource} : {query}')

        self._init_client(service, resource)
        self._check_config(service, resource)

        response = getattr(self.client[service], resource).stat({
            'domain_id': domain_id,
            'query': query
        }, metadata=self.transaction.get_connection_meta())

        return self._change_message(response)

    @staticmethod
    def _change_message(message):
        return MessageToDict(message, preserving_proto_field_name=True)
