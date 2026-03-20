"""
Sub-environment deployment monitor.
"""
import asyncio
from typing import Awaitable, Callable, Dict, Optional, Set

from kubernetes import client, watch
from kubernetes.client.rest import ApiException

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
            else:
                logger.debug("Sub-env monitor unchanged: total=%s", len(next_namespaces))
        except Exception as e:
            logger.error(f"Failed to refresh sub-environment list: {e}")

    async def add_namespace(self, namespace_name: str):
        """Add a namespace to monitoring."""
        if not self._is_candidate_namespace(namespace_name):
            return
        async with self._lock:
            self.monitored_namespaces.add(namespace_name)
        logger.info(f"Added namespace to sub-env monitor: {namespace_name}")

    async def remove_namespace(self, namespace_name: str):
        """Remove a namespace from monitoring."""
        async with self._lock:
            removed = namespace_name in self.monitored_namespaces
            self.monitored_namespaces.discard(namespace_name)
        if removed:
            logger.info(f"Removed namespace from sub-env monitor: {namespace_name}")

    async def _is_monitored(self, namespace_name: str) -> bool:
        async with self._lock:
            return namespace_name in self.monitored_namespaces

    async def _dispatch_callback(self, callback: Optional[DeploymentCallback], deployment_name: str, namespace_name: str):
        if not callback:
            return
        await callback(deployment_name, namespace_name)

    async def _handle_deployment_event(self, event_type: str, deployment_name: str, namespace_name: str):
        if event_type == "ADDED":
            await self._dispatch_callback(self.on_created, deployment_name, namespace_name)
        elif event_type == "DELETED":
            await self._dispatch_callback(self.on_deleted, deployment_name, namespace_name)

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
                            timeout_seconds=300,
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
