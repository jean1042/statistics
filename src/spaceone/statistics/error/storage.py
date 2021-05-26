from spaceone.core.error import *


class ERROR_INVALID_PLUGIN_VERSION(ERROR_INVALID_ARGUMENT):
    _message = 'Plugin version is invalid. (plugin_id = {plugin_id}, version = {version})'

class ERROR_INVALID_PLUGIN_OPTIONS(ERROR_INTERNAL_API):
    _message = 'The options received from the plugin is invalid. (reason = {reason})'
