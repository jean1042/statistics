from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class Scheduled(EmbeddedDocument):
    cron = StringField(max_length=1024, default=None, null=True)
    interval = IntField(min_value=1, max_value=60, default=None, null=True)
    minutes = ListField(IntField(), default=None, null=True)
    hours = ListField(IntField(), default=None, null=True)

    def to_dict(self):
        return self.to_mongo()


class PluginInfo(EmbeddedDocument):
    plugin_id = StringField(max_length=40)
    version = FloatField()
    options = DictField()
    secret_data = DictField()


class Storage(MongoModel):
    storage_id = StringField(max_length=40, generate_id='sto', unique=True)
    name = StringField(max_length=255)
    state = StringField(max_length=20, default='ENABLED', choices=('ENABLED', 'DISABLED'))
    capability = DictField(required=True)
    # tags = ListField(EmbeddedDocumentField(ScheduleTag))
    tags = DictField()
    plugin_info = DictField(EmbeddedDocumentField(PluginInfo))
    domain_id = StringField(max_length=255)
    user_id = StringField(max_length=255)
    created_at = DateTimeField(auto_now_add=True)

    meta = {
        'updatable_fields': [
            'schedule',
            'state',
            'tags',
            'last_scheduled_at'
        ],
        'minimal_fields': [
            'schedule_id',
            'topic',
            'state'
        ],
        'ordering': [
            'topic'
        ],
        'indexes': [
            'schedule_id',
            'topic',
            'state',
            'domain_id',
            ('tags.key', 'tags.value')
        ]
    }
