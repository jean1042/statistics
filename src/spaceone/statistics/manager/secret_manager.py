import logging

from spaceone.core import cache
from spaceone.core.manager import BaseManager
from spaceone.statistics.error import *
from spaceone.statistics.connector.secret_connector import SecretConnector
from pprint import pprint
_LOGGER = logging.getLogger(__name__)


class SecretManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.secret_connector: SecretConnector = self.locator.get_connector('SecretConnector')

    def list_secrets(self, query, domain_id):
        return self.secret_connector.list_secrets(query, domain_id)

    def get_secret_data(self, secret_id, domain_id):
        response = self.secret_connector.get_secret_data(secret_id, domain_id)
        return response['data']

    def get_plugin_secret_data(self, secret_id, supported_schema, domain_id):
        secret_query = self._make_query(supported_schema=supported_schema, secret_id=secret_id)
        response = self.list_secrets(secret_query, domain_id)

        if response.get('total_count', 0) == 0:
            raise ERROR_NOT_FOUND(key='plugin_info.secret_id', value=secret_id)

        return self.get_secret_data(secret_id, domain_id)

    def get_resource_secret_data(self, resource_id, secret_filter, domain_id):
        secret_query = self._make_query(**secret_filter)

        response = self.list_secrets(secret_query, domain_id)

        if response.get('total_count', 0) == 0:
            raise ERROR_RESOURCE_SECRETS_NOT_EXISTS(resource_id=resource_id)

        result = response['results'][0]
        secret_id = result['secret_id']
        schema = result.get('schema')

        return self.get_secret_data(secret_id, domain_id), schema

    def get_plugin_secret(self, plugin_id, secret_id, provider, capability, domain_id):
        use_resource_secret = capability.get('use_resource_secret', False)
        supported_schema = capability.get('supported_schema', [])

        self._check_plugin_secret(use_resource_secret, secret_id, provider)

        if use_resource_secret:
            secret_query = self._make_query(supported_schema=supported_schema, provider=provider)
        else:
            secret_query = self._make_query(supported_schema=supported_schema, secret_id=secret_id)

        response = self.list_secrets(secret_query, domain_id)

        if response.get('total_count', 0) == 0:
            if use_resource_secret:
                raise ERROR_SUPPORTED_SECRETS_NOT_EXISTS(plugin_id=plugin_id)
            else:
                raise ERROR_NOT_FOUND(key='plugin_info.secret_id', value=secret_id)

        result = response['results'][0]
        secret_id = result['secret_id']
        schema = result.get('schema')

        return self.get_secret_data(secret_id, domain_id), schema

    def create_secret(self, name, data, secret_type, domain_id):
        """
        params:
            name : <str>,
            data : <dict>,
            secret_type : 'CREDENTIALS',
            domain_id : <str>
        Return: secret_id <str>
        """
        print("### secret_type ###")
        print(secret_type)
        params = {'name': name, 'data': data, 'secret_type': secret_type, 'domain_id': domain_id}
        return self.secret_connector.create_secret(params=params)

    @staticmethod
    def _check_plugin_secret(use_resource_secret, secret_id, provider):
        if use_resource_secret:
            if provider is None:
                raise ERROR_REQUIRED_PARAMETER(key='plugin_info.provider')
        else:
            if secret_id is None:
                raise ERROR_REQUIRED_PARAMETER(key='plugin_info.secret_id')

    @staticmethod
    def _make_query(**secret_filter):
        schema = secret_filter.get('schema')
        secret_id = secret_filter.get('secret_id')
        service_account_id = secret_filter.get('service_account_id')
        secrets = secret_filter.get('secrets')

        query = {
            'filter': []
        }

        if schema:
            query['filter'].append({
                'k': 'schema',
                'v': schema,
                'o': 'in'
            })

        if secret_id:
            query['filter'].append({
                'k': 'secret_id',
                'v': secret_id,
                'o': 'eq'
            })

        if service_account_id:
            query['filter'].append({
                'k': 'service_account_id',
                'v': service_account_id,
                'o': 'eq'
            })

        if secrets:
            query['filter'].append({
                'k': 'secret_id',
                'v': secrets,
                'o': 'in'
            })

        return query
