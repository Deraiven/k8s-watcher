"""
Sub-environment deployment monitor.
"""
import asyncio
from typing import Awaitable, Callable, Dict, Optional, Set

from kubernetes import client, watch
from kubernetes.client.rest import ApiException

from ..config.settings import app_config
from ..utils.logger import setup_logger

logger = setup_logger(__name__)

DeploymentCallback = Callable[[str, str], Awaitable[None]]


class SubEnvironmentMonitor:
    """Watch deployment events for monitored sub-environment namespaces."""

    def __init__(
        self,
        zadig_manager,
        namespace_prefix: str,
        excluded_namespaces: Set[str],
        refresh_interval_seconds: int = 60,
        on_created: Optional[DeploymentCallback] = None,
        on_deleted: Optional[DeploymentCallback] = None,
    ):
        self.apps_v1 = client.AppsV1Api()
        self.zadig_manager = zadig_manager
        self.namespace_prefix = namespace_prefix
        self.excluded_namespaces = excluded_namespaces
        self.refresh_interval_seconds = refresh_interval_seconds
        self.on_created = on_created
        self.on_deleted = on_deleted

        self._shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self.monitored_namespaces: Set[str] = set()
        self.deployment_tasks: Set[asyncio.Task] = set()
        self._seen_deployments: Set[str] = set()
        self._catchup_tasks: Dict[str, asyncio.Task] = {}

    def _is_candidate_namespace(self, namespace_name: str) -> bool:
        if namespace_name in self.excluded_namespaces:
            return False
        return namespace_name.startswith(self.namespace_prefix)

    async def refresh_monitored_namespaces(self):
        """Refresh monitored namespaces from Zadig environments."""
        try:
            environments = await self.zadig_manager.get_environments()
            if not environments:
                logger.warning("No environments returned from Zadig")
                return

            next_namespaces: Set[str] = set()
            for env in environments:
                namespace_name = env.get("namespace") or env.get("env_key")
                if not namespace_name:
                    continue
                if self._is_candidate_namespace(namespace_name):
                    next_namespaces.add(namespace_name)

            async with self._lock:
                added = next_namespaces - self.monitored_namespaces
                removed = self.monitored_namespaces - next_namespaces
                self.monitored_namespaces = next_namespaces

            if added or removed:
                logger.info(
                    "Sub-env monitor updated: total=%s added=%s removed=%s",
                    len(next_namespaces),
                    len(added),
                    len(removed),
                )
                # Reconcile already-existing deployments for newly added namespaces.
                # This covers the gap where deployments were created before monitor started tracking.
                for namespace_name in added:
                    task = asyncio.create_task(self._reconcile_existing_deployments(namespace_name))
                    self.deployment_tasks.add(task)
                    task.add_done_callback(lambda t: self.deployment_tasks.discard(t))
            else:
                logger.debug("Sub-env monitor unchanged: total=%s", len(next_namespaces))
        except Exception as e:
            logger.error(f"Failed to refresh sub-environment list: {e}")

    async def add_namespace(self, namespace_name: str):
        """Add a namespace to monitoring."""
        if not self._is_candidate_namespace(namespace_name):
            return
        async with self._lock:
            is_new = namespace_name not in self.monitored_namespaces
            self.monitored_namespaces.add(namespace_name)
        logger.info(f"Added namespace to sub-env monitor: {namespace_name}")
        if is_new:
            await self._reconcile_existing_deployments(namespace_name)
            # Short catch-up window: deployments may appear a few seconds after namespace ADDED.
            await self._schedule_namespace_catchup(namespace_name)

    async def remove_namespace(self, namespace_name: str):
        """Remove a namespace from monitoring."""
        catchup_task = None
        async with self._lock:
            removed = namespace_name in self.monitored_namespaces
            self.monitored_namespaces.discard(namespace_name)
            catchup_task = self._catchup_tasks.pop(namespace_name, None)
            namespace_prefix = f"{namespace_name}/"
            self._seen_deployments = {key for key in self._seen_deployments if not key.startswith(namespace_prefix)}
        if catchup_task and not catchup_task.done():
            catchup_task.cancel()
        if removed:
            logger.info(f"Removed namespace from sub-env monitor: {namespace_name}")

    async def _is_monitored(self, namespace_name: str) -> bool:
        async with self._lock:
            return namespace_name in self.monitored_namespaces

    async def _mark_seen_and_check_new(self, namespace_name: str, deployment_name: str) -> bool:
        key = f"{namespace_name}/{deployment_name}"
        async with self._lock:
            if key in self._seen_deployments:
                return False
            self._seen_deployments.add(key)
            return True

    async def _clear_seen(self, namespace_name: str, deployment_name: str):
        key = f"{namespace_name}/{deployment_name}"
        async with self._lock:
            self._seen_deployments.discard(key)

    async def _dispatch_callback(self, callback: Optional[DeploymentCallback], deployment_name: str, namespace_name: str):
        if not callback:
            return
        await callback(deployment_name, namespace_name)

    async def _handle_deployment_event(self, event_type: str, deployment_name: str, namespace_name: str):
        if event_type == "ADDED":
            is_new = await self._mark_seen_and_check_new(namespace_name, deployment_name)
            if is_new:
                await self._dispatch_callback(self.on_created, deployment_name, namespace_name)
        elif event_type == "DELETED":
            await self._clear_seen(namespace_name, deployment_name)
            await self._dispatch_callback(self.on_deleted, deployment_name, namespace_name)

    async def _schedule_namespace_catchup(self, namespace_name: str):
        async with self._lock:
            existing = self._catchup_tasks.get(namespace_name)
            if existing and not existing.done():
                return
            task = asyncio.create_task(self._namespace_catchup_loop(namespace_name))
            self._catchup_tasks[namespace_name] = task
            self.deployment_tasks.add(task)
            task.add_done_callback(lambda t: self.deployment_tasks.discard(t))

    async def _namespace_catchup_loop(self, namespace_name: str):
        try:
            # Retry a few times to capture deployments created right after namespace creation.
            for _ in range(6):
                if self._shutdown_event.is_set():
                    return
                if not await self._is_monitored(namespace_name):
                    return
                await self._reconcile_existing_deployments(namespace_name)
                await asyncio.sleep(5)
        finally:
            async with self._lock:
                current = self._catchup_tasks.get(namespace_name)
                if current is asyncio.current_task():
                    self._catchup_tasks.pop(namespace_name, None)

    async def _reconcile_existing_deployments(self, namespace_name: str):
        """Replay create callback for deployments that already exist in namespace."""
        try:
            deployment_list = await asyncio.to_thread(
                self.apps_v1.list_namespaced_deployment,
                namespace=namespace_name,
            )
            items = deployment_list.items or []
            if not items:
                return

            logger.info(
                "Reconciling existing deployments for namespace=%s count=%s",
                namespace_name,
                len(items),
            )
            for deployment in items:
                deployment_name = deployment.metadata.name
                await self._handle_deployment_event("ADDED", deployment_name, namespace_name)
        except ApiException as e:
            if e.status == 404:
                logger.info(f"Namespace {namespace_name} not found during deployment reconcile")
                return
            logger.error(f"Failed to reconcile deployments for namespace {namespace_name}: {e}")
        except Exception as e:
            logger.error(f"Unexpected reconcile error for namespace {namespace_name}: {e}")

    async def _refresh_loop(self):
        """Background loop to periodically refresh monitored namespaces."""
        while not self._shutdown_event.is_set():
            await self.refresh_monitored_namespaces()
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.refresh_interval_seconds,
                )
            except asyncio.TimeoutError:
                continue

    async def watch_deployments(self):
        """Watch deployment events and process monitored namespaces only."""
        logger.info("Starting deployment watch for sub-environments")

        while not self._shutdown_event.is_set():
            try:
                event_queue = asyncio.Queue()
                loop = asyncio.get_event_loop()

                def run_watch():
                    try:
                        deployment_watch = watch.Watch()
                        for event in deployment_watch.stream(
                            self.apps_v1.list_deployment_for_all_namespaces,
                            timeout_seconds=app_config.watch_stream_timeout_seconds,
                        ):
                            future = asyncio.run_coroutine_threadsafe(
                                event_queue.put(event),
                                loop,
                            )
                            future.result()
                    except Exception as e:
                        asyncio.run_coroutine_threadsafe(
                            event_queue.put({"type": "ERROR", "error": e}),
                            loop,
                        ).result()
                    finally:
                        asyncio.run_coroutine_threadsafe(
                            event_queue.put({"type": "WATCH_ENDED"}),
                            loop,
                        ).result()

                import threading

                watch_thread = threading.Thread(target=run_watch, daemon=True)
                watch_thread.start()

                while not self._shutdown_event.is_set():
                    try:
                        event = await asyncio.wait_for(event_queue.get(), timeout=5.0)

                        if event.get("type") == "ERROR":
                            raise event["error"]
                        if event.get("type") == "WATCH_ENDED":
                            logger.info("Deployment watch stream ended, restarting...")
                            break

                        deployment = event["object"]
                        event_type = event["type"]
                        namespace_name = deployment.metadata.namespace
                        deployment_name = deployment.metadata.name

                        if event_type not in ("ADDED", "DELETED"):
                            continue
                        if not await self._is_monitored(namespace_name):
                            continue

                        logger.info(
                            "Sub-env deployment event: %s ns=%s deployment=%s",
                            event_type,
                            namespace_name,
                            deployment_name,
                        )

                        task = asyncio.create_task(
                            self._handle_deployment_event(event_type, deployment_name, namespace_name)
                        )
                        self.deployment_tasks.add(task)
                        task.add_done_callback(lambda t: self.deployment_tasks.discard(t))
                    except asyncio.TimeoutError:
                        continue
            except ApiException as e:
                if e.status == 410:
                    logger.warning("Deployment resource version too old, restarting watch")
                    await asyncio.sleep(1)
                else:
                    logger.error(f"Deployment watch API exception: {e}")
                    await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Unexpected deployment watch error: {e}")
                await asyncio.sleep(5)

    async def run(self):
        """Run monitor loops."""
        await self.refresh_monitored_namespaces()
        await asyncio.gather(
            self._refresh_loop(),
            self.watch_deployments(),
        )

    async def shutdown(self):
        """Shutdown monitor."""
        self._shutdown_event.set()
        if self.deployment_tasks:
            await asyncio.gather(*self.deployment_tasks, return_exceptions=True)
