import unittest
from unittest.mock import AsyncMock, patch

from src.config.settings import apollo_config
from src.managers.apollo_manager import ApolloAPIError, ApolloManager


class ApolloTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        with patch.object(apollo_config, 'token', 'test-token'):
            self.manager = ApolloManager()
        self.source = self.manager._cluster_path('orders', self.manager.reference_env)
        self.target = self.manager._cluster_path('orders', 'test99')
        self.suffix = '/namespaces/web.orders'
        self.cluster = False
        self.release = None
        self.items = []
        self.writes = []
        self.fail_item_once = False

        async def api(session, method, path, **kwargs):
            if method == 'GET':
                if path == self.source + self.suffix:
                    return {'items': [
                        {'key': 'host', 'value': 'test33/api/TEST33'},
                        {'key': 'queue', 'value': 'https://sqs.ap-southeast-1.amazonaws.com/1/test33'},
                    ]}
                if path == self.target:
                    return {'name': 'test99'} if self.cluster else None
                if path == self.target + self.suffix:
                    return {'items': list(self.items)}
                if path.endswith('/releases/latest'):
                    return self.release
                self.fail('Unexpected read: ' + path)
            self.writes.append((method, path, kwargs['json']))
            if path.endswith('/clusters'):
                self.cluster = True
                return {'name': 'test99'}
            if path.endswith('/items'):
                if self.fail_item_once:
                    self.fail_item_once = False
                    raise ApolloAPIError(method, path, 503)
                self.items.append(kwargs['json'])
                return kwargs['json']
            if path.endswith('/releases'):
                self.release = {'id': 1}
                return self.release
            self.fail('Unexpected write: ' + path)

        self.api = AsyncMock(side_effect=api)
        self.manager._request = self.api

    async def test_create_and_replay(self):
        self.assertTrue(await self.manager.ensure_subenv_app_config('orders', 'test99'))
        self.assertEqual(self.items[0]['value'], 'test99/api/TEST99')
        self.assertIn('test33', self.items[1]['value'])
        self.assertEqual(len(self.writes), 4)
        await self.manager.ensure_subenv_app_config('orders', 'test99')
        self.assertEqual(len(self.writes), 4)
        self.assertFalse(any('secret' in call.args[2] for call in self.api.call_args_list))

    async def test_existing_items_and_release_preserved(self):
        self.cluster = True
        self.release = {'id': 1}
        self.items = [{'key': 'host', 'value': 'custom'}]
        await self.manager.ensure_subenv_app_config('orders', 'test99')
        self.assertEqual(self.items[0]['value'], 'custom')
        self.assertEqual(len(self.writes), 1)
        self.assertTrue(self.writes[0][1].endswith('/items'))

    async def test_partial_failure_can_resume(self):
        self.fail_item_once = True
        with self.assertRaises(ApolloAPIError):
            await self.manager.ensure_subenv_app_config('orders', 'test99')
        self.assertIsNone(self.release)
        await self.manager.ensure_subenv_app_config('orders', 'test99')
        self.assertEqual(len(self.items), 2)
        self.assertIsNotNone(self.release)
        self.assertEqual(sum(path.endswith('/clusters') for _, path, _ in self.writes), 1)

    async def test_aliases(self):
        sync = AsyncMock(return_value=True)
        self.manager._sync_single_app_config = sync
        await self.manager.ensure_subenv_app_config('backoffice-v1-web-api', 'test99')
        sync.assert_awaited_once_with('backoffice-v1-web', 'test99')
        self.assertEqual(self.manager._resolve_target_app_ids('backoffice-v1-web-app'),
                         ['backoffice-v2-webapp', 'backoffice-v1-web'])
        self.assertEqual(self.manager._resolve_target_app_ids('beep-v1-web'),
                         ['beep-v1-web', 'beep-v1-webapp'])
        self.assertFalse(await self.manager.ensure_subenv_app_config('bo-v1-assets', 'test99'))

    async def test_protected_cluster_and_unsupported_delete(self):
        with self.assertRaises(ValueError):
            await self.manager.ensure_subenv_app_config('orders', self.manager.reference_env)
        with self.assertRaises(NotImplementedError):
            await self.manager.delete_cluster_config('test99')
        self.api.assert_not_awaited()

    async def test_missing_target_namespace_is_error(self):
        original = self.api.side_effect

        async def missing(session, method, path, **kwargs):
            if path == self.target + self.suffix:
                return None
            return await original(session, method, path, **kwargs)

        self.api.side_effect = missing
        with self.assertRaisesRegex(RuntimeError, 'repair in Portal'):
            await self.manager.ensure_subenv_app_config('orders', 'test99')
        self.assertIsNone(self.release)

    def test_required_token(self):
        with patch.object(apollo_config, 'token', None):
            with self.assertRaisesRegex(ValueError, 'APOLLO_API_TOKEN'):
                ApolloManager()

    async def test_auth_header(self):
        async with self.manager._session() as session:
            self.assertEqual(session.headers['Authorization'], 'test-token')
            self.assertEqual(session.timeout.total, 30)


class TransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_status_and_redaction(self):
        with patch.object(apollo_config, 'token', 'test-token'):
            manager = ApolloManager()
        session = unittest.mock.MagicMock()
        response = AsyncMock()
        session.request.return_value.__aenter__.return_value = response
        response.status = 403
        with self.assertRaises(ApolloAPIError) as caught:
            await manager._request(session, 'POST', '/test', json={'value': 'private'})
        self.assertNotIn('private', str(caught.exception))
        self.assertEqual(session.request.call_count, 1)
        self.assertFalse(session.request.call_args.kwargs['allow_redirects'])
        response.status = 404
        self.assertIsNone(await manager._request(session, 'GET', '/test', missing_ok=True))
        response.status = 429
        session.request.reset_mock()
        with patch('src.managers.apollo_manager.asyncio.sleep', new_callable=AsyncMock):
            with self.assertRaises(ApolloAPIError):
                await manager._request(session, 'GET', '/test')
        self.assertEqual(session.request.call_count, 3)


if __name__ == '__main__':
    unittest.main()
