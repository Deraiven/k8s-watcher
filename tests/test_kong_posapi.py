import unittest
from unittest.mock import AsyncMock

from src.managers.kong_manager import KongManager


class PosapiTests(unittest.IsolatedAsyncioTestCase):
    async def test_posapi_routes_and_shared_plugin(self):
        manager = KongManager()
        manager._ensure_service = AsyncMock()
        manager._ensure_route = AsyncMock()
        manager._ensure_pre_function_plugin = AsyncMock()
        await manager.ensure_subenv_deployment_routes('backoffice-v1-web-posapi', 'test54')
        service = manager._ensure_service.call_args.kwargs
        self.assertEqual(service['service_name'], 'backoffice-v1-web-posapi-test54')
        self.assertEqual(service['host'], 'backoffice-v1-web-posapi.test54')
        self.assertEqual(service['port'], 80)
        route = manager._ensure_route.call_args.kwargs['route_payload']
        self.assertEqual(route['hosts'], ['www.backoffice.test54.shub.us'])
        self.assertTrue(route['preserve_host'])
        self.assertEqual(manager._ensure_pre_function_plugin.call_args.args[1],
                         'backoffice-v1-web-posapi-test54')
        manager._ensure_service.assert_awaited_once()

    async def test_api_host_unchanged(self):
        manager = KongManager()
        manager._ensure_service = AsyncMock()
        manager._ensure_route = AsyncMock()
        manager._ensure_pre_function_plugin = AsyncMock()
        await manager.ensure_subenv_deployment_routes('backoffice-v1-web-api', 'test54')
        route = manager._ensure_route.call_args.kwargs['route_payload']
        self.assertEqual(route['hosts'], ['backoffice-api.test54.shub.us'])
        self.assertFalse(route['preserve_host'])

    async def test_delete_only_posapi_resources_in_order(self):
        manager = KongManager()
        operations = []

        async def routes(session, name, env):
            operations.append(('routes', name, env))

        async def plugins(session, name):
            operations.append(('plugins', name))

        async def service(session, name):
            operations.append(('service', name))

        manager._delete_service_routes_by_env = routes
        manager._delete_service_plugins = plugins
        manager._delete_service_if_exists = service
        await manager.remove_subenv_deployment_routes('backoffice-v1-web-posapi', 'test54')
        self.assertEqual(operations, [
            ('routes', 'backoffice-v1-web-posapi-test54', 'test54'),
            ('plugins', 'backoffice-v1-web-posapi-test54'),
            ('service', 'backoffice-v1-web-posapi-test54'),
        ])
