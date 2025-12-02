import asyncio
import functools
import logging
import kubernetes.client
import kubernetes.watch
from kubernetes import client, config

logger = logging.getLogger(__name__)

class DeploymentInformer:
    def __init__(self, api_client: kubernetes.client.AppsV1Api, namespace: str = None):
        self.api_client = api_client
        self.namespace = namespace
        self._should_run = asyncio.Event()
        self._handlers = {'add': [], 'update': [], 'delete': []}
        self._resource_version = None
        self._loop_task = None

    def add_event_handler(self, add_func=None, update_func=None, delete_func=None):
        if add_func:
            self._handlers['add'].append(add_func)
        if update_func:
            self._handlers['update'].append(update_func)
        if delete_func:
            self._handlers['delete'].append(delete_func)

    async def _watch_loop(self):
        while self._should_run.is_set():
            try:
                # Use list_deployment_for_all_namespaces or list_namespaced_deployment
                if self.namespace:
                    resp = await self.api_client.list_namespaced_deployment(
                        namespace=self.namespace,
                        watch=False,
                        _async=True
                    )
                else:
                    resp = await self.api_client.list_deployment_for_all_namespaces(
                        watch=False,
                        _async=True
                    )

                self._resource_version = resp.metadata.resource_version
                
                # Use kubernetes.watch.Watch for streaming events
                w = kubernetes.watch.Watch()
                
                watch_args = {
                    'resource_version': self._resource_version,
                    'timeout_seconds': 300 # Watch for 5 minutes then re-list
                }
                if self.namespace:
                    async_watcher = functools.partial(self.api_client.list_namespaced_deployment, namespace=self.namespace, watch=True, _async=True, **watch_args)
                else:
                    async_watcher = functools.partial(self.api_client.list_deployment_for_all_namespaces, watch=True, _async=True, **watch_args)

                async for event in w.stream(async_watcher):
                    event_type = event['type'].lower()
                    deployment = event['object']
                    
                    if event_type in self._handlers:
                        for handler in self._handlers[event_type]:
                            asyncio.create_task(handler(deployment))

            except client.ApiException as e:
                if e.status == 401:
                    logger.error("Kubernetes API Unauthorized. Check credentials.")
                    self._should_run.clear()
                elif e.status == 410: # Resource version expired
                    logger.warning("Resource version expired, restarting watch from scratch.")
                    self._resource_version = None # Reset to fetch all resources again
                else:
                    logger.error(f"Kubernetes API Error in DeploymentInformer: {e}")
                await asyncio.sleep(5) # Wait before retrying
            except asyncio.CancelledError:
                logger.info("DeploymentInformer watch loop cancelled.")
                break
            except Exception as e:
                logger.error(f"Unexpected error in DeploymentInformer: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def start(self):
        logger.info("Starting DeploymentInformer...")
        self._should_run.set()
        self._loop_task = asyncio.create_task(self._watch_loop())

    def stop(self):
        logger.info("Stopping DeploymentInformer...")
        self._should_run.clear()
        if self._loop_task:
            self._loop_task.cancel()
            self._loop_task = None