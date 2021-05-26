from spaceone.api.statistics.v1 import storage_pb2, storage_pb2_grpc
from spaceone.core.pygrpc import BaseAPI

class Storage(BaseAPI, storage_pb2_grpc.StorageServicer):

    pb2 = storage_pb2
    pb2_grpc = storage_pb2_grpc

    def register(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('StorageService', metadata) as storage_service:
            return self.locator.get_info('StorageInfo', storage_service.register(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('StorageService', metadata) as storage_service:
            return self.locator.get_info('StorageInfo', storage_service.update(params))

    def update_plugin(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('StorageService', metadata) as storage_service:
            return self.locator.get_info('StorageInfo', storage_service.update_plugin(params))

    def verify_plugin(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('StorageService', metadata) as storage_service:
            return self.locator.get_info('StorageInfo', storage_service.verify_plugin(params))

    def enable(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('StorageService', metadata) as storage_service:
            return self.locator.get_info('StorageInfo', storage_service.enable(params))

    def disable(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('StorageService', metadata) as storage_service:
            return self.locator.get_info('StorageInfo', storage_service.disable(params))

    def deregister(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('StorageService', metadata) as storage_service:
            storage_service.deregister(params)
            return self.locator.get_info('StorageInfo', storage_service.deregister(params))

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('StorageService', metadata) as storage_service:
            return self.locator.get_info('StorageInfo', storage_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('StorageService', metadata) as storage_service:
            storage_vos, total_count = storage_service.list(params)
            return self.locator.get_info('StorageInfo', storage_vos,
                                         total_count, minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('StorageService', metadata) as storage_service:
            return self.locator.get_info('StatisticsInfo', storage_service.stat(params))
