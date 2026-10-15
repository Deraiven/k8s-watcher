import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

# Importing the real scheduler starts a background thread. Keep tests offline.
with patch.dict('sys.modules', {'src.utils.schedule': SimpleNamespace(
    AdvancedScheduler=SimpleNamespace(daily=lambda **kwargs: lambda fn: fn)
)}):
    from src.managers.zadig_manager import ZadigManager


DAY = 86400
NOW = 1800000000


class CleanupTests(unittest.IsolatedAsyncioTestCase):
    async def run_cleanup(self, age, updated, **fields):
        # Avoid creating Kubernetes clients or making any network calls.
        manager = object.__new__(ZadigManager)
        env = dict(env_key='test99', namespace='test99', status='running', update_time=updated)
        env.update(fields)
        manager.get_environments = AsyncMock(return_value=[env])
        manager._get_namespace_created_timestamp = AsyncMock(return_value=NOW - age)
        manager._set_environment_sleep = AsyncMock(return_value=True)
        manager._clear_environment = AsyncMock(return_value=True)
        with patch('src.managers.zadig_manager.time.time', return_value=NOW):
            await manager.cleanup_expired_environments()
        return manager

    async def test_sleep_requires_both_strict_thresholds(self):
        cases = [
            (8 * DAY, NOW - DAY - 1, True),
            (8 * DAY, NOW - DAY, False),
            (8 * DAY, NOW - 3600, False),
            (7 * DAY, NOW - 2 * DAY, False),
            (6 * DAY, NOW - 2 * DAY, False),
            (8 * DAY, NOW + DAY, False),
            (8 * DAY, (NOW - DAY - 1) * 1000, True),
            (8 * DAY, str(NOW - 2 * DAY), True),
        ]
        for age, updated, expected in cases:
            with self.subTest(age=age, updated=updated):
                manager = await self.run_cleanup(age, updated)
                self.assertEqual(manager._set_environment_sleep.await_count, int(expected))
                if expected:
                    manager._set_environment_sleep.assert_awaited_once_with('test99', 'enable')
                manager._clear_environment.assert_not_awaited()

    async def test_invalid_update_time_does_not_sleep(self):
        for updated in (None, 0, '', 'invalid', -1):
            with self.subTest(updated=updated):
                manager = await self.run_cleanup(8 * DAY, updated)
                manager._set_environment_sleep.assert_not_awaited()
                manager._clear_environment.assert_not_awaited()

    async def test_protected_environments_unchanged(self):
        for fields in (dict(env_key='test17'), dict(production=True), dict(namespace='production')):
            manager = await self.run_cleanup(8 * DAY, NOW - 2 * DAY, **fields)
            manager._set_environment_sleep.assert_not_awaited()
            manager._clear_environment.assert_not_awaited()

    async def test_sleeping_deletion_requires_more_than_three_days(self):
        for age, expected in ((3 * DAY + 1, True), (3 * DAY, False), (2 * DAY, False), (4 * DAY, True)):
            manager = await self.run_cleanup(30 * DAY, NOW - age, status='sleeping')
            self.assertEqual(manager._clear_environment.await_count, int(expected))
            manager._set_environment_sleep.assert_not_awaited()

    async def test_sleeping_invalid_timestamp_is_not_deleted(self):
        for updated in (None, 0, '', 'invalid', -1, NOW + DAY):
            manager = await self.run_cleanup(30 * DAY, updated, status='sleeping')
            manager._clear_environment.assert_not_awaited()
            manager._set_environment_sleep.assert_not_awaited()

    async def test_sleeping_millisecond_timestamp(self):
        manager = await self.run_cleanup(30 * DAY, (NOW - 3 * DAY - 1) * 1000, status='sleeping')
        manager._clear_environment.assert_awaited_once_with('test99')
        manager._set_environment_sleep.assert_not_awaited()

    async def test_whitelist_moves_from_test50_to_test54(self):
        for status in ('running', 'sleeping'):
            protected = await self.run_cleanup(
                30 * DAY, NOW - 8 * DAY, env_key='test54', namespace='test54', status=status)
            protected._set_environment_sleep.assert_not_awaited()
            protected._clear_environment.assert_not_awaited()
            ordinary = await self.run_cleanup(
                30 * DAY, NOW - 8 * DAY, env_key='test50', namespace='test50', status=status)
            if status == 'running':
                ordinary._set_environment_sleep.assert_awaited_once_with('test50', 'enable')
                ordinary._clear_environment.assert_not_awaited()
            else:
                ordinary._clear_environment.assert_awaited_once_with('test50')
                ordinary._set_environment_sleep.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
