from mongoengine import *
from datetime import datetime

from spaceone.core.model.mongo_model import MongoModel
from spaceone.statistics.error import *


class PluginInfo(EmbeddedDocument):
    plugin_id = StringField(max_length=40)
    version = StringField()
    options = DictField(default=None)
    secret_id = StringField()
    metadata = DictField(default=None)


class StorageTag(EmbeddedDocument):
    key = StringField(max_length=255)
    value = StringField(max_length=255)


class Storage(MongoModel):
    storage_id = StringField(max_length=40, generate_id='sto', unique=True)
    name = StringField(max_length=255)
    state = StringField(max_length=20, default='ENABLED', choices=('ENABLED', 'DISABLED', 'NONE'))
    capability = DictField()
    tags = DictField()
    plugin_info = EmbeddedDocumentField(PluginInfo, default=None, null=True)
    domain_id = StringField(max_length=255)
    user_id = StringField(max_length=255)
    created_at = DateTimeField(auto_now_add=True)

    meta = {
        'updatable_fields': [
            'name',
            'state',
            'tags'
        ],
        'minimal_fields': [
            'storage_id',
            'state',
            'name'
        ],
        'indexes': [
            'storage_id',
            'domain_id',
        ]
    }

    def deregister(self):
        if self.state == 'DISABLED':
            raise ERROR_RESOURCE_ALREADY_DISABLED(resource_type='Storage', resource_id={self.storage_id})

        self.update({
            'state': 'DISABLED',
            'deleted_at': datetime.utcnow()
        })
