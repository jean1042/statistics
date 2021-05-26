import functools
from spaceone.api.statistics.v1 import storage_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.statistics.model.storage_model import Storage, PluginInfo

__all__ = ['StorageInfo']

'''
def ScheduledInfo(vo: Scheduled):
    info = {
        'cron': vo.cron,
        'interval': vo.interval,
        'hours': vo.hours,
        'minutes': vo.minutes
    }
    return schedule_pb2.Scheduled(**info)
'''


def PluginInfo(plugin_vo: PluginInfo):
    info = {
        'plugin_id': plugin_vo.plugin_id,
        'version': plugin_vo.version,
        'secret_id': plugin_vo.secret_id
    }


def StorageInfo(storage_vo: Storage, minimal=False):
    info = {
        'storage_id': storage_vo.storage_id,
        'name': storage_vo.name,
        'state': storage_vo.state
    }

    if not minimal:
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
    return storage_pb2.StoragesInfo(results=list(
        map(functools.partial(StorageInfo, **kwargs), storage_vos)), total_count=total_count)
