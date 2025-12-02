"""
Kubernetes informer-like implementation for Python
"""
import asyncio
import time
from typing import Callable, Dict, Any, Optional
from kubernetes import client, watch
from kubernetes.client.rest import ApiException

from .logger import setup_logger

logger = setup_logger(__name__)


class NamespaceInformer:
    """
    Informer-like implementation for watching Kubernetes namespaces
    Inspired by client-go's informer pattern
    """
    
    def __init__(self, v1_client: client.CoreV1Api):
        self.v1 = v1_client
        self.cache: Dict[str, Any] = {}
        self.resource_version: Optional[str] = None
        self.handlers = {
            'add': [],
            'update': [],
            'delete': []
        }
        self._stop = False
        self._resync_period = 300  # 5 minutes
        self._last_resync = 0
        
    def add_event_handler(self, 
                         add_func: Optional[Callable] = None,
                         update_func: Optional[Callable] = None, 
                         delete_func: Optional[Callable] = None):
        """Add event handlers for namespace events"""
        if add_func:
            self.handlers['add'].append(add_func)
        if update_func:
            self.handlers['update'].append(update_func)
        if delete_func:
            self.handlers['delete'].append(delete_func)
    
    async def _list_and_watch(self):
        """List all namespaces and then watch for changes"""
        while not self._stop:
            try:
                # Initial list to populate cache and get resourceVersion
                if not self.resource_version:
                    logger.info("Performing initial namespace list")
                    namespace_list = await asyncio.to_thread(
                        self.v1.list_namespace
                    )
                    
                    # Update cache and trigger add events
                    for ns in namespace_list.items:
                        key = ns.metadata.name
                        self.cache[key] = ns
                        await self._handle_event('add', ns)
                    
                    self.resource_version = namespace_list.metadata.resource_version
                    logger.info(f"Initial list completed, resourceVersion: {self.resource_version}")
                
                # Watch for changes
                w = watch.Watch()
                logger.info("Starting watch stream")
                
                # Use asyncio.to_thread to prevent blocking
                async for event in self._async_watch(w):
                    event_type = event['type'].lower()
                    namespace = event['object']
                    key = namespace.metadata.name
                    
                    if event_type == 'added':
                        self.cache[key] = namespace
                        await self._handle_event('add', namespace)
                    elif event_type == 'modified':
                        old = self.cache.get(key)
                        self.cache[key] = namespace
                        await self._handle_event('update', namespace, old)
                    elif event_type == 'deleted':
                        self.cache.pop(key, None)
                        await self._handle_event('delete', namespace)
                    
                    # Update resource version
                    if hasattr(namespace.metadata, 'resource_version'):
                        self.resource_version = namespace.metadata.resource_version
                
                # Check if resync is needed
                current_time = time.time()
                if current_time - self._last_resync > self._resync_period:
                    logger.info("Performing periodic resync")
                    self.resource_version = None  # Force re-list
                    self._last_resync = current_time
                
            except ApiException as e:
                if e.status == 410:  # Gone - resourceVersion too old
                    logger.warning("ResourceVersion too old, will re-list")
                    self.resource_version = None
                    await asyncio.sleep(1)
                else:
                    logger.error(f"API error: {e}")
                    await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Unexpected error in watch loop: {e}")
                await asyncio.sleep(5)
    
    async def _async_watch(self, watcher):
        """Async wrapper for kubernetes watch"""
        loop = asyncio.get_event_loop()
        
        def watch_generator():
            try:
                for event in watcher.stream(
                    self.v1.list_namespace,
                    resource_version=self.resource_version,
                    timeout_seconds=300
                ):
                    yield event
            except StopIteration:
                return
            except Exception as e:
                logger.error(f"Error in watch stream: {e}")
                raise
        
        # Convert blocking generator to async
        for event in watch_generator():
            yield event
            # Allow other coroutines to run
            await asyncio.sleep(0)
    
    async def _handle_event(self, event_type: str, obj: Any, old_obj: Any = None):
        """Handle events by calling registered handlers"""
        handlers = self.handlers.get(event_type, [])
        for handler in handlers:
            try:
                if event_type == 'update':
                    await asyncio.create_task(handler(obj, old_obj))
                else:
                    await asyncio.create_task(handler(obj))
            except Exception as e:
                logger.error(f"Error in event handler: {e}")
    
    def list(self) -> Dict[str, Any]:
        """Get all namespaces from cache"""
        return dict(self.cache)
    
    def get(self, name: str) -> Optional[Any]:
        """Get a namespace from cache"""
        return self.cache.get(name)
    
    async def start(self):
        """Start the informer"""
        logger.info("Starting namespace informer")
        await self._list_and_watch()
    
    def stop(self):
        """Stop the informer"""
        logger.info("Stopping namespace informer")
        self._stop = True