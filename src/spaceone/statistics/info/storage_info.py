import functools
from spaceone.api.statistics.v1 import storage_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.statistics.model.storage_model import Storage, PluginInfo

__all__ = ['StorageInfo', 'StoragesInfo', 'PluginInfo']


def PluginInfo(plugin_info: PluginInfo):
    if plugin_info:
        info = {
            'plugin_id': plugin_info.plugin_id,
            'version': plugin_info.version,
            'secret_id': plugin_info.secret_id,
            'options': change_struct_type(plugin_info.options),
            'metadata': change_struct_type(plugin_info.metadata)

        }
        return storage_pb2.PluginInfo(**info)
    return None


def StorageInfo(storage_vo: Storage, minimal=False):
    info = {
        'storage_id': storage_vo.storage_id,
        'name': storage_vo.name,
        'state': storage_vo.state
    }

    if not minimal:
        print(storage_vo.to_dict())
        info.update({
            'tags': change_struct_type(utils.tags_to_dict(storage_vo.tags)),
            'capability': change_struct_type(storage_vo.capability) if storage_vo.capability else None,
            'plugin_info': PluginInfo(storage_vo.plugin_info) if storage_vo.plugin_info else None,
            'user_id': storage_vo.user_id,
            'domain_id': storage_vo.domain_id,
            'created_at': utils.datetime_to_iso8601(storage_vo.created_at)
        })

    return storage_pb2.StorageInfo(**info)


def StoragesInfo(storage_vos, total_count, **kwargs):
    results = list(map(functools.partial(StorageInfo, **kwargs), storage_vos))

    return storage_pb2.StoragesInfo(results=results, total_count=total_count)
