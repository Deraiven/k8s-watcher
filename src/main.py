"""
Main controller for namespace watcher
"""
import asyncio
import signal
import sys
from typing import Optional, Set, Dict, List
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

from .config.settings import app_config
from .managers.aws_manager import AWSManager
from .managers.dns_manager import CloudflareDNSManager
from .managers.cert_manager import CertificateManager
from .managers.apollo_manager import ApolloManager
from .managers.kong_manager import KongManager
from .managers.zadig_manager import ZadigManager
from .managers.redis_state_manager import RedisStateManager
from .utils.logger import setup_logger
from .utils.schedule import AdvancedScheduler

logger = setup_logger(__name__)


class NamespaceWatcher:
    """Main controller for watching Kubernetes namespace events"""
    
    def __init__(self):
        # Initialize Kubernetes client
        if app_config.in_cluster:
            config.load_incluster_config()
        else:
            config.load_kube_config()
        
        self.v1 = client.CoreV1Api()
        self.watcher = watch.Watch()
        self.stored_namespaces: Set[str] = set()
        self.processing_namespaces: Set[str] = set()  # Track namespaces being processed
        self.creation_tasks: Dict[str, List[asyncio.Task]] = {}  # Track creation tasks for cancellation
        self.namespace_tasks: Set[asyncio.Task] = set()  # Track all namespace processing tasks
        
        # Initialize managers
        self.aws_manager = AWSManager() if app_config.enable_aws_resources else None
        self.dns_manager = CloudflareDNSManager() if app_config.enable_dns_management else None
        self.cert_manager = CertificateManager() if app_config.enable_cert_management else None
        self.apollo_manager = ApolloManager() if app_config.enable_apollo_config else None
        self.kong_manager = KongManager() if app_config.enable_kong_routes else None
        self.zadig_manager = ZadigManager() if app_config.enable_zadig_workflow else None
        self.state_manager = RedisStateManager()
        
        # Shutdown event
        self._shutdown_event = asyncio.Event()
    
    async def initialize(self):
        """Initialize the watcher with existing namespaces"""
        try:
            # Connect to Redis
            await self.state_manager.connect()
            # 查看定时任务
            AdvancedScheduler.start()
            logger.info("List the schedule job")
            AdvancedScheduler.list_tasks()
            # Get tracked namespaces from Redis
            redis_tracked = await self.state_manager.get_tracked_namespaces()
            logger.info(f"Found {len(redis_tracked)} namespaces in Redis state")
            
            # Get existing namespaces from Kubernetes
            namespaces = await asyncio.to_thread(self.v1.list_namespace)
            k8s_namespaces = set()
            namespaces_to_reconcile = []
            
            for ns in namespaces.items:
                if self._should_track_namespace(ns):
                    ns_name = ns.metadata.name
                    k8s_namespaces.add(ns_name)
                    self.stored_namespaces.add(ns_name)
                    
                    # Check if needs reconciliation
                    if ns_name not in redis_tracked:
                        logger.info(f"Namespace {ns_name} exists in K8s but not in Redis state")
                        namespaces_to_reconcile.append(ns_name)
                    else:
                        # Check if needs periodic reconciliation
                        needs_check = await self.state_manager.get_namespaces_needing_reconciliation(hours=24)
                        if ns_name in needs_check:
                            namespaces_to_reconcile.append(ns_name)
                            
                elif ns.metadata.name in app_config.excluded_namespaces and ns.metadata.name.startswith(app_config.namespace_prefix):
                    logger.info(f"Excluding namespace {ns.metadata.name} from tracking")
            
            # Check for namespaces in Redis but not in K8s (deleted while watcher was down)
            deleted_namespaces = redis_tracked - k8s_namespaces
            for ns in deleted_namespaces:
                logger.warning(f"Namespace {ns} exists in Redis but not in K8s, marking as deleted")
                await self.state_manager.mark_namespace_deleted(ns)
            
            # Check for orphaned resources
            orphaned = await self.state_manager.find_orphaned_resources()
            if orphaned:
                logger.warning(f"Found orphaned resources for {len(orphaned)} deleted namespaces")
                for ns, resources in orphaned.items():
                    logger.warning(f"Namespace {ns} has orphaned resources: {resources}")
            
            logger.info(f"Initialized with namespaces [{' '.join(self.stored_namespaces)}]")
            logger.info(f"Initialized with {len(self.stored_namespaces)} existing namespaces")
            
            # Reconcile namespaces that need it
            if namespaces_to_reconcile:
                logger.info(f"Starting reconciliation for {len(namespaces_to_reconcile)} namespaces")
                await self._reconcile_namespaces(namespaces_to_reconcile)
            
        except Exception as e:
            logger.error(f"Failed to initialize: {e}")
            raise
    
    def _should_track_namespace(self, namespace) -> bool:
        """Check if namespace should be tracked"""
        namespace_name = namespace.metadata.name
        
        # Exclude specific namespaces
        if namespace_name in app_config.excluded_namespaces:
            return False
        
        return (
            namespace_name.startswith(app_config.namespace_prefix) and
            namespace.metadata.labels and
            namespace.metadata.labels.get(app_config.namespace_label_key) == app_config.namespace_label_value
        )
    
    async def _reconcile_namespaces(self, namespaces: List[str]):
        """Reconcile resources for existing namespaces"""
        logger.info(f"Reconciling {len(namespaces)} namespaces")
        
        # Process namespaces in parallel
        tasks = []
        for namespace_name in namespaces:
            # Create a task that includes Redis state update
            task = asyncio.create_task(self._reconcile_single_namespace(namespace_name))
            tasks.append(task)
        
        # Wait for all reconciliations to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Log results
        success_count = 0
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Failed to reconcile {namespaces[i]}: {result}")
            else:
                success_count += 1
        
        logger.info(f"Reconciliation completed: {success_count}/{len(namespaces)} successful")
    
    async def _reconcile_single_namespace(self, namespace_name: str):
        """Reconcile a single namespace with Redis state update"""
        try:
            # Create resources
            await self._create_namespace_resources(namespace_name)
            
            # Update Redis state
            await self.state_manager.mark_namespace_created(namespace_name)
            logger.debug(f"Updated Redis state for {namespace_name}")
            
        except Exception as e:
            logger.error(f"Failed to reconcile {namespace_name}: {e}")
            raise
    
    async def _create_namespace_resources(self, namespace_name: str):
        """Create resources for a namespace (used by both creation and reconciliation)"""
        # Group 1: Certificate first (cert-manager will handle DNS validation)
        cert_task = None
        if self.cert_manager:
            cert_task = asyncio.create_task(self.cert_manager.create_certificate(namespace_name))
        
        # Group 2: DNS record depends on certificate
        async def create_dns_after_cert():
            try:
                if cert_task:
                    await cert_task
                if self.dns_manager:
                    await self.dns_manager.create_dns_record(namespace_name)
            except asyncio.CancelledError:
                logger.info(f"DNS creation cancelled for {namespace_name}")
                raise
        
        dns_task = asyncio.create_task(create_dns_after_cert())
        
        # Group 3: All other tasks can run in parallel immediately
        parallel_tasks = []
        
        # Create AWS resources
        if self.aws_manager:
            parallel_tasks.append(asyncio.create_task(self.aws_manager.create_sqs_queues(namespace_name)))
            parallel_tasks.append(asyncio.create_task(self.aws_manager.create_sns_topic(namespace_name)))
        
        # Create Kong routes
        if self.kong_manager:
            parallel_tasks.append(asyncio.create_task(self.kong_manager.create_routes(namespace_name)))
        
        # Update Zadig workflows
        if self.zadig_manager:
            parallel_tasks.append(asyncio.create_task(self.zadig_manager.update_workflow_environments('add', namespace_name)))
        
        # Execute all tasks in parallel
        all_tasks = []
        if cert_task:
            all_tasks.append(cert_task)
        if dns_task:
            all_tasks.append(dns_task)
        all_tasks.extend(parallel_tasks)
        
        results = await asyncio.gather(*all_tasks, return_exceptions=True)
        
        # Log any failures
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Task {i} failed for {namespace_name}: {result}")
    
    async def handle_namespace_created(self, namespace_name: str):
        """Handle namespace creation event"""
        logger.info(f"Processing namespace creation: {namespace_name}")
        
        # Mark as processing
        self.processing_namespaces.add(namespace_name)
        self.creation_tasks[namespace_name] = []
        
        try:
            # Create all resources (with cancellation support)
            await self._create_namespace_resources_with_tracking(namespace_name)
            
            # Mark as completed
            self.stored_namespaces.add(namespace_name)
            
            # Update Redis state
            await self.state_manager.mark_namespace_created(namespace_name)
            
            logger.info(f"Completed namespace creation processing: {namespace_name}")
            
        except asyncio.CancelledError:
            logger.info(f"Creation process cancelled for {namespace_name}")
            raise
        except Exception as e:
            logger.error(f"Failed to create resources for {namespace_name}: {e}")
            raise
        finally:
            # Always remove from processing set and tasks
            self.processing_namespaces.discard(namespace_name)
            self.creation_tasks.pop(namespace_name, None)
    
    async def _create_namespace_resources_with_tracking(self, namespace_name: str):
        """Create resources with task tracking for cancellation support"""
        # Group 1: Certificate first (cert-manager will handle DNS validation)
        cert_task = None
        if self.cert_manager:
            cert_task = asyncio.create_task(self.cert_manager.create_certificate(namespace_name))
            if namespace_name in self.creation_tasks:
                self.creation_tasks[namespace_name].append(cert_task)
        
        # Group 2: DNS record depends on certificate
        async def create_dns_after_cert():
            try:
                if cert_task:
                    await cert_task
                if self.dns_manager:
                    await self.dns_manager.create_dns_record(namespace_name)
            except asyncio.CancelledError:
                logger.info(f"DNS creation cancelled for {namespace_name}")
                raise
        
        dns_task = asyncio.create_task(create_dns_after_cert())
        if namespace_name in self.creation_tasks:
            self.creation_tasks[namespace_name].append(dns_task)
        
        # Group 3: All other tasks can run in parallel immediately
        parallel_tasks = []
        
        # Create AWS resources
        if self.aws_manager:
            task = asyncio.create_task(self.aws_manager.create_sqs_queues(namespace_name))
            parallel_tasks.append(task)
            if namespace_name in self.creation_tasks:
                self.creation_tasks[namespace_name].append(task)
            
            task = asyncio.create_task(self.aws_manager.create_sns_topic(namespace_name))
            parallel_tasks.append(task)
            if namespace_name in self.creation_tasks:
                self.creation_tasks[namespace_name].append(task)
        
        # Create Kong routes
        if self.kong_manager:
            task = asyncio.create_task(self.kong_manager.create_routes(namespace_name))
            parallel_tasks.append(task)
            if namespace_name in self.creation_tasks:
                self.creation_tasks[namespace_name].append(task)
        
        # Update Zadig workflows
        if self.zadig_manager:
            task = asyncio.create_task(self.zadig_manager.update_workflow_environments('add', namespace_name))
            parallel_tasks.append(task)
            if namespace_name in self.creation_tasks:
                self.creation_tasks[namespace_name].append(task)
        
        # Execute all tasks in parallel
        all_tasks = []
        if cert_task:
            all_tasks.append(cert_task)
        if dns_task:
            all_tasks.append(dns_task)
        all_tasks.extend(parallel_tasks)
        
        results = await asyncio.gather(*all_tasks, return_exceptions=True)
        
        # Log any failures
        cancelled_count = 0
        for i, result in enumerate(results):
            if isinstance(result, asyncio.CancelledError):
                cancelled_count += 1
            elif isinstance(result, Exception):
                logger.error(f"Task {i} failed for {namespace_name}: {result}")
        
        if cancelled_count > 0:
            raise asyncio.CancelledError(f"Creation cancelled for {namespace_name}: {cancelled_count} tasks were cancelled")
    
    async def handle_namespace_deleted(self, namespace_name: str):
        """Handle namespace deletion event"""
        logger.debug(f"Starting deletion for {namespace_name}. In stored: {namespace_name in self.stored_namespaces}, In processing: {namespace_name in self.processing_namespaces}")
        
        # Check if namespace is being processed or already stored
        if namespace_name not in self.stored_namespaces and namespace_name not in self.processing_namespaces:
            logger.info(f"Namespace {namespace_name} not found in stored or processing, skipping deletion")
            return
        
        # Cancel creation if still processing
        if namespace_name in self.processing_namespaces:
            logger.warning(f"Namespace {namespace_name} is still being created, cancelling creation process")
            
            # Cancel all creation tasks
            if namespace_name in self.creation_tasks:
                for task in self.creation_tasks[namespace_name]:
                    if not task.done():
                        task.cancel()
                        logger.debug(f"Cancelled task for {namespace_name}: {task}")
                
                # Wait for all tasks to complete cancellation
                await asyncio.gather(*self.creation_tasks[namespace_name], return_exceptions=True)
            
            # Wait for processing flag to be cleared
            while namespace_name in self.processing_namespaces:
                await asyncio.sleep(0.1)
        
        # Check if we should clean up partial resources
        if namespace_name not in self.stored_namespaces:
            logger.info(f"Namespace {namespace_name} creation was cancelled, checking for partial resources to clean up")
            # Continue to deletion to clean up any partially created resources
        
        logger.info(f"Processing namespace deletion: {namespace_name}")
        
        # Group 1: Delete DNS record first (certificate depends on DNS)
        if self.dns_manager:
            try:
                await self.dns_manager.delete_dns_record(namespace_name)
            except Exception as e:
                logger.error(f"Failed to delete DNS record: {e}")
        
        # Group 2: Delete other resources in parallel
        parallel_tasks = []
        
        # Delete certificate (after DNS is deleted)
        if self.cert_manager:
            parallel_tasks.append(self.cert_manager.delete_certificate(namespace_name))
        
        # Delete AWS resources
        if self.aws_manager:
            parallel_tasks.append(self.aws_manager.delete_sqs_queues(namespace_name))
            parallel_tasks.append(self.aws_manager.delete_sns_topic(namespace_name))
        
        # Delete Kong routes
        if self.kong_manager:
            parallel_tasks.append(self.kong_manager.delete_routes(namespace_name))
        
        # Update Zadig workflows
        if self.zadig_manager:
            parallel_tasks.append(self.zadig_manager.update_workflow_environments('delete', namespace_name))
        
        #clear apolli config 
        if self.apollo_manager:
            parallel_tasks.append(self.apollo_manager.delete_cluster_config(namespace_name))
        # Execute parallel tasks
        if parallel_tasks:
            results = await asyncio.gather(*parallel_tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Delete task {i} failed: {result}")
        
        self.stored_namespaces.discard(namespace_name)
        
        # Update Redis state
        await self.state_manager.mark_namespace_deleted(namespace_name)
        
        logger.info(f"Completed namespace deletion processing: {namespace_name}")
    
    async def watch_namespaces(self):
        """Watch for namespace events"""
        logger.info("Starting namespace watch")
        last_heartbeat = asyncio.get_event_loop().time()
        
        while not self._shutdown_event.is_set():
            try:
                # Create a queue to pass events from the blocking thread to async
                event_queue = asyncio.Queue()
                loop = asyncio.get_event_loop()
                
                # Function to run in thread
                def run_watch():
                    try:
                        w = watch.Watch()
                        # Use timeout to prevent indefinite blocking
                        for event in w.stream(self.v1.list_namespace, timeout_seconds=300):  # 5 minutes timeout
                            # Put event in queue
                            future = asyncio.run_coroutine_threadsafe(
                                event_queue.put(event),
                                loop
                            )
                            # Wait for the future to complete
                            future.result()
                    except Exception as e:
                        # Put exception as special event
                        asyncio.run_coroutine_threadsafe(
                            event_queue.put({'type': 'ERROR', 'error': e}),
                            loop
                        ).result()
                    finally:
                        # Signal that watch has ended
                        asyncio.run_coroutine_threadsafe(
                            event_queue.put({'type': 'WATCH_ENDED'}),
                            loop
                        ).result()
                
                # Start watch in thread
                import threading
                watch_thread = threading.Thread(target=run_watch)
                watch_thread.daemon = True
                watch_thread.start()
                
                # Process events from queue
                while not self._shutdown_event.is_set():
                    try:
                        # Wait for event with timeout
                        event = await asyncio.wait_for(event_queue.get(), timeout=5.0)
                        
                        # Check for special events
                        if event.get('type') == 'ERROR':
                            raise event['error']
                        elif event.get('type') == 'WATCH_ENDED':
                            logger.info("Watch stream ended, restarting...")
                            break  # Break inner loop to restart watch
                        
                        namespace = event['object']
                        event_type = event['type']
                        
                        logger.debug(f"Received event: {event_type} for namespace: {namespace.metadata.name}")
                        
                        if event_type == "ADDED":
                            should_track = self._should_track_namespace(namespace)
                            in_stored = namespace.metadata.name in self.stored_namespaces
                            in_processing = namespace.metadata.name in self.processing_namespaces
                            logger.debug(f"ADDED event - should_track: {should_track}, in_stored: {in_stored}, in_processing: {in_processing}, name: {namespace.metadata.name}")
                            
                            if should_track and not in_stored and not in_processing:
                                logger.info(f"Creating the namespace {namespace.metadata.name}")
                                # Create task for parallel processing
                                task = asyncio.create_task(self.handle_namespace_created(namespace.metadata.name))
                                self.namespace_tasks.add(task)
                                task.add_done_callback(lambda t: self.namespace_tasks.discard(t))
                        
                        elif event_type == "DELETED": 
                            logger.info(f"Deleting the namespace {namespace.metadata.name}")
                            # Create task for parallel processing
                            task = asyncio.create_task(self.handle_namespace_deleted(namespace.metadata.name))
                            self.namespace_tasks.add(task)
                            task.add_done_callback(lambda t: self.namespace_tasks.discard(t))
                            
                    except asyncio.TimeoutError:
                        # No events in queue, check heartbeat
                        current_time = asyncio.get_event_loop().time()
                        if current_time - last_heartbeat > 60:  # Log every minute
                            logger.debug("Namespace watcher is alive, waiting for events...")
                            last_heartbeat = current_time
                        continue
                        
            except ApiException as e:
                if e.status == 410:  # Resource version too old
                    logger.warning("Resource version too old, restarting watch")
                    await asyncio.sleep(1)
                else:
                    logger.error(f"API exception: {e}")
                    await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                await asyncio.sleep(5)
    
    
    async def shutdown(self):

        """Graceful shutdown"""
        logger.info("stop the scheduler")
        AdvancedScheduler.stop()
        logger.info("Shutting down namespace watcher")
        self._shutdown_event.set()
        self.watcher.stop()
        
        # Wait for all namespace tasks to complete
        if self.namespace_tasks:
            logger.info(f"Waiting for {len(self.namespace_tasks)} namespace tasks to complete")
            await asyncio.gather(*self.namespace_tasks, return_exceptions=True)
        
        # Disconnect from Redis
        await self.state_manager.disconnect()
    
    async def run(self):
        """Run the namespace watcher"""
        try:
            await self.initialize()
            await self.watch_namespaces()
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            raise
        finally:
            await self.shutdown()


def signal_handler(sig, _frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {sig}")
    asyncio.create_task(watcher.shutdown())
    sys.exit(0)


async def main():
    """Main entry point"""
    global watcher
    watcher = NamespaceWatcher()
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await watcher.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())