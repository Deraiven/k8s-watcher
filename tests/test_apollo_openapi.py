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

    async def test_protected_cluster_and_missing_admin_credentials(self):
        with self.assertRaises(ValueError):
            await self.manager.ensure_subenv_app_config('orders', self.manager.reference_env)
        self.manager.hmac_username = None
        with self.assertRaises(ValueError):
            await self.manager.delete_cluster_config('test99')
        self.api.assert_not_awaited()

    async def test_admin_cleanup_pagination_and_exact_name(self):
        self.manager.hmac_username = 'watcher'
        self.manager.hmac_secret = 'test-secret'
        admin = AsyncMock(side_effect=[
            [{'appId': 'orders'}], [{'appId': 'other'}], [],
            {'appId': 'orders', 'name': 'test99'}, None, None,
        ])
        self.manager._admin_request = admin
        self.assertTrue(await self.manager.delete_cluster_config('test99'))
        requests = [(c.args[1], c.args[2]) for c in admin.call_args_list]
        self.assertEqual(requests[:3], [('GET', '/apps?page=0&size=100'),
                                      ('GET', '/apps?page=1&size=100'),
                                      ('GET', '/apps?page=2&size=100')])
        deletes = [path for method, path in requests if method == 'DELETE']
        self.assertEqual(deletes, ['/apps/orders/clusters/test99?operator=namespace-watcher'])

    async def test_admin_delete_protected_names(self):
        for env in ('test33', 'test17', 'default', 'prod', 'test1-extra', '../test1'):
            with self.subTest(env=env), self.assertRaises(ValueError):
                await self.manager.delete_cluster_config(env)

    async def test_admin_repeated_page_aborts_before_delete(self):
        self.manager.hmac_username = 'watcher'
        self.manager.hmac_secret = 'test-secret'
        admin = AsyncMock(return_value=[{'appId': 'orders'}])
        self.manager._admin_request = admin
        with self.assertRaisesRegex(RuntimeError, 'pagination repeated'):
            await self.manager.delete_cluster_config('test99')
        self.assertTrue(all(c.args[1] == 'GET' for c in admin.call_args_list))

    async def test_admin_http_signing_and_empty_delete(self):
        import base64
        import hashlib
        import hmac

        self.manager.hmac_username = 'watcher'
        self.manager.hmac_secret = 'test-secret'
        path = '/apps/orders/clusters/test99?operator=a%2Bb+user'
        date = 'Fri, 09 Oct 2026 01:00:00 GMT'
        message = f'date: {date}\nrequest-line: DELETE {path} HTTP/1.1'
        signature = base64.b64encode(hmac.new(b'test-secret', message.encode(), hashlib.sha256).digest()).decode()
        session = unittest.mock.MagicMock()
        response = AsyncMock()
        session.request.return_value.__aenter__.return_value = response
        response.status = 200
        with patch('src.managers.apollo_manager.formatdate', return_value=date):
            await self.manager._admin_request(session, 'DELETE', path)
        kwargs = session.request.call_args.kwargs
        self.assertIn(f'signature="{signature}"', kwargs['headers']['Authorization'])
        self.assertEqual(session.request.call_args.args[1].raw_path_qs, path)
        self.assertFalse(kwargs['allow_redirects'])
        response.json.assert_not_awaited()
        response.status = 403
        with self.assertRaisesRegex(RuntimeError, 'HTTP 403'):
            await self.manager._admin_request(session, 'DELETE', path, missing_ok=True)
        response.status = 404
        self.assertIsNone(await self.manager._admin_request(session, 'DELETE', path, missing_ok=True))

    async def test_missing_target_namespace_attached_before_copy_and_release(self):
        original = self.api.side_effect
        attached = False

        async def attach(app, env):
            nonlocal attached
            self.assertEqual((app, env), ('orders', 'test99'))
            attached = True

        self.manager._ensure_web_namespace = AsyncMock(side_effect=attach)

        async def missing(session, method, path, **kwargs):
            if path == self.target + self.suffix and not attached:
                return None
            return await original(session, method, path, **kwargs)

        self.api.side_effect = missing
        await self.manager.ensure_subenv_app_config('orders', 'test99')
        self.manager._ensure_web_namespace.assert_awaited_once_with('orders', 'test99')
        self.assertEqual(len(self.items), 2)
        self.assertIsNotNone(self.release)
        await self.manager.ensure_subenv_app_config('orders', 'test99')
        self.manager._ensure_web_namespace.assert_awaited_once()

    async def test_namespace_admin_payload_and_duplicate_race(self):
        self.manager.hmac_username = 'watcher'
        self.manager.hmac_secret = 'test-secret'
        dto = {'appId': 'orders', 'clusterName': 'test99', 'namespaceName': 'web.orders'}
        for post_result in (dto, ApolloAPIError('POST', '/namespaces', 400)):
            admin = AsyncMock(side_effect=[None, post_result, dto])
            self.manager._admin_request = admin
            await self.manager._ensure_web_namespace('orders', 'test99')
            call = admin.call_args_list[1]
            self.assertEqual(call.args[1:3], ('POST', '/apps/orders/clusters/test99/namespaces'))
            self.assertEqual(call.kwargs['json'], dict(dto,
                dataChangeCreatedBy='namespace-watcher', dataChangeLastModifiedBy='namespace-watcher'))

    async def test_namespace_admin_auth_failure_stops_sync(self):
        original = self.api.side_effect

        async def missing(session, method, path, **kwargs):
            if path == self.target + self.suffix:
                return None
            return await original(session, method, path, **kwargs)

        self.api.side_effect = missing
        self.manager.hmac_username = 'watcher'
        self.manager.hmac_secret = 'test-secret'
        admin = AsyncMock(side_effect=ApolloAPIError('GET', '/namespaces', 403))
        self.manager._admin_request = admin
        with self.assertRaises(ApolloAPIError):
            await self.manager.ensure_subenv_app_config('orders', 'test99')
        self.assertFalse(self.items)
        self.assertIsNone(self.release)

    def test_required_token(self):
        with patch.object(apollo_config, 'token', None):
            with self.assertRaisesRegex(ValueError, 'APOLLO_API_TOKEN'):
                ApolloManager()

    async def test_auth_header(self):
        async with self.manager._session() as session:
            self.assertEqual(session.headers['Authorization'], 'test-token')
            self.assertEqual(session.timeout.total, 30)

    async def test_token_surrounding_whitespace_removed(self):
        with patch.object(apollo_config, 'token', ' \r\ntest-token\n\t'):
            manager = ApolloManager()
        async with manager._session() as session:
            self.assertEqual(session.headers['Authorization'], 'test-token')

    def test_embedded_token_controls_rejected_without_secret(self):
        for control in ('\r', '\n', '\t', '\x00', '\x7f'):
            with self.subTest(control=repr(control)):
                with patch.object(apollo_config, 'token', f'private{control}token'):
                    with self.assertRaisesRegex(ValueError, 'embedded control character') as caught:
                        ApolloManager()
                self.assertNotIn('private', str(caught.exception))

    def test_whitespace_only_token_rejected(self):
        with patch.object(apollo_config, 'token', ' \r\n\t'):
            with self.assertRaisesRegex(ValueError, 'APOLLO_API_TOKEN is required'):
                ApolloManager()


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
