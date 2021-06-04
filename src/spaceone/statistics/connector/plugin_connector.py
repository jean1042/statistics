import logging

from google.protobuf.json_format import MessageToDict

from spaceone.core.connector import BaseConnector
from spaceone.core import pygrpc
from spaceone.core.utils import parse_endpoint
from spaceone.core.error import *
from spaceone.statistics.error.resource import *

__all__ = ['PluginConnector']

_LOGGER = logging.getLogger(__name__)


class PluginConnector(BaseConnector):

    def __init__(self, transaction, config):
        super().__init__(transaction, config)
        self._check_config()
        self.initialize(config.get('endpoint'))

    def initialize(self, endpoint):
        static_endpoint = self.config.get('endpoint')
        if static_endpoint:
            endpoint = static_endpoint.get('v1')
        e = parse_endpoint(endpoint)
        self.client = pygrpc.client(endpoint=f'{e.get("hostname")}:{e.get("port")}', version='plugin')

    def init_client(self, service, resource):
        if service not in self.client:
            if service not in self.config:
                raise ERROR_REQUIRED_PARAMETER(resource_type=f'{service}.{resource}')

            e = parse_endpoint(self.config[service])
            if e.get('path') is None:
                raise ERROR_CONNECTOR_CONFIGURATION(backend=self.__class__.__name__)

            self.client[service] = pygrpc.client(endpoint=f'{e.get("hostname")}:{e.get("port")}')

    def init(self, options):
        response = self.client.Plugin.init({
            'options': options
        }, metadata=self.transaction.get_connection_meta())

        return self._change_message(response)

    def _check_config(self):
        if 'endpoint' not in self.config:
            raise ERROR_CONNECTOR_CONFIGURATION(backend=self.__class__.__name__)

        if len(self.config['endpoint']) > 1:
            raise ERROR_CONNECTOR_CONFIGURATION(backend=self.__class__.__name__)

    def get_plugin_endpoint(self, plugin_id, version, domain_id, **kwargs):
        response = self.client.Plugin.get_plugin_endpoint({
            'plugin_id': plugin_id,
            'version': version,
            'labels': kwargs.get('labels', {}),
            'domain_id': domain_id
        }, metadata=self.transaction.get_connection_meta())

        return response.endpoint

    @staticmethod
    def _change_message(message):
        return MessageToDict(message, preserving_proto_field_name=True)
