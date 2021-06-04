from spaceone.core.error import *


class ERROR_INVALID_PLUGIN_VERSION(ERROR_INVALID_ARGUMENT):
    _message = 'Plugin version is invalid. (plugin_id = {plugin_id}, version = {version})'

class ERROR_INVALID_PLUGIN_OPTIONS(ERROR_INTERNAL_API):
    _message = 'The options received from the plugin is invalid. (reason = {reason})'

class ERROR_RESOURCE_SECRETS_NOT_EXISTS(ERROR_INVALID_ARGUMENT):
    _message = 'There are no secrets in the resources. (resource_id = {resource_id})'

class ERROR_SUPPORTED_SECRETS_NOT_EXISTS(ERROR_INVALID_ARGUMENT):
    _message = 'There are no secrets that support plugins. (plugin_id = {plugin_id}, provider = {provider})'

class ERROR_REQUIRED_PARAMETER(ERROR_INVALID_ARGUMENT):
    _message = 'Required parameter. (key = {key})'

class ERROR_INVALID_RESOURCE_TYPE(ERROR_INVALID_ARGUMENT):
    _message = 'Resource type is undefined. (resource_type = {resource_type})'

class ERROR_STORAGE_OPTION(ERROR_INVALID_ARGUMENT):
    _message = 'Only one storage option can be set.'

class ERROR_RESOURCE_ALREADY_DISABLED(ERROR_INVALID_ARGUMENT):
    _message = '{resource_type} has already been deleted. ({resource_id})'
