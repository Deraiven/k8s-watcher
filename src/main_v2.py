"""
Main controller for namespace watcher V2 - Using Informer Pattern
"""
import asyncio
import signal
import sys
from typing import Optional, Set, Dict, List
from kubernetes import client, config

from .config.settings import app_config, zadig_config
from .managers.aws_manager import AWSManager
from .managers.dns_manager import DNSManager
from .managers.cert_manager import CertificateManager
from .managers.apollo_manager import ApolloManager
from .managers.kong_manager import KongManager
from .managers.zadig_manager import ZadigManager
from .managers.redis_state_manager import RedisStateManager
from .utils.logger import setup_logger
from .utils.k8s_informer import NamespaceInformer
from .utils.k8s_deployment_informer import DeploymentInformer # New Import

logger = setup_logger(__name__)


class NamespaceWatcherV2:
    """Main controller for watching Kubernetes namespace events using Informer pattern"""
    
    def __init__(self):
        # Initialize Kubernetes client
        if app_config.in_cluster:
            config.load_incluster_config()
        else:
            config.load_kube_config()
        
        self.v1 = client.CoreV1Api()
        self.apps_v1 = client.AppsV1Api() # New Kubernetes API client
        self.informer = NamespaceInformer(self.v1)
        self.deployment_informer = DeploymentInformer(self.apps_v1) # New Deployment Informer
        
        self.stored_namespaces: Set[str] = set()
        self.processing_namespaces: Set[str] = set()
        self.creation_tasks: Dict[str, List[asyncio.Task]] = {}
        self.namespace_tasks: Set[asyncio.Task] = set()
        
        # Initialize managers
        self.aws_manager = AWSManager() if app_config.enable_aws_resources else None
        self.dns_manager = DNSManager() if app_config.enable_dns_management else None
        self.cert_manager = CertificateManager() if app_config.enable_cert_management else None
        self.apollo_manager = ApolloManager() if app_config.enable_apollo_config else None
        self.kong_manager = KongManager() if app_config.enable_kong_routes else None
        self.zadig_manager = ZadigManager() if app_config.enable_zadig_workflow else None
        self.state_manager = RedisStateManager()
        
        # Shutdown event
        self._shutdown_event = asyncio.Event()
        
        # Setup informer handlers
        self._setup_informer_handlers()
    
    def _setup_informer_handlers(self):
        """Setup event handlers for the informer"""
        self.informer.add_event_handler(
            add_func=self._on_namespace_added,
            update_func=self._on_namespace_updated,
            delete_func=self._on_namespace_deleted
        )
        # Add Deployment informer handlers
        self.deployment_informer.add_event_handler(
            add_func=self._on_deployment_added,
            update_func=None, # Only handle add events for now
            delete_func=None
        )
    
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

    async def _on_deployment_added(self, deployment):
        """Handle deployment add event to add Git trigger"""
        namespace_name = deployment.metadata.namespace
        deployment_name = deployment.metadata.name

        # Check if the namespace is one we are tracking (already stored or processing)
        if namespace_name not in self.stored_namespaces and namespace_name not in self.processing_namespaces:
            logger.debug(f"Deployment {deployment_name} in untracked namespace {namespace_name}, skipping.")
            return
        
        logger.info(f"Deployment added: {deployment_name} in namespace {namespace_name}")

        if not self.zadig_manager:
            logger.warning("Zadig manager is not enabled, skipping Git trigger addition.")
            return

        # Extract Git information from image tag
        image_tag_found = False
        extracted_repo_name = None
        extracted_branch = None

        containers = deployment.spec.template.spec.containers or []
        for container in containers:
            image = container.image
            if ':' in image:
                full_image_name, tag = image.rsplit(':', 1)
                # Extract repo name from full_image_name (e.g., after the last '/')
                repo_name_parts = full_image_name.rsplit('/', 1)
                extracted_repo_name = repo_name_parts[-1] # This assumes the last part is the repo name

                if '-' in tag:
                    # Expecting format like CB-15608-324a4fb2
                    tag_parts = tag.rsplit('-', 1)
                    if len(tag_parts) > 1:
                        extracted_branch = tag_parts[0] # Branch is everything before the last hyphen
                        logger.info(f"Extracted branch '{extracted_branch}' from image tag '{tag}' for repo '{extracted_repo_name}'.")
                        image_tag_found = True
                        break # Found branch, no need to check other containers
            if image_tag_found:
                break


        if not image_tag_found:
            logger.warning(
                f"Could not extract Git branch from image tag for Deployment {deployment_name} in {namespace_name}. "
                f"Falling back to configured default values if available for repo_name and branch or skipping trigger creation."
            )
            # Use configured templates or defaults as fallback
            resolved_repo_name = zadig_config.repo_name_template.format(namespace_name=deployment_name)
            resolved_branch = zadig_config.default_git_branch
            
            if resolved_repo_name == zadig_config.repo_name_template.format(namespace_name=deployment_name) and resolved_branch == zadig_config.default_git_branch:
                logger.warning(f"Using default repo_name '{resolved_repo_name}' and branch '{resolved_branch}' for Git trigger.")
            elif resolved_repo_name == zadig_config.repo_name_template.format(namespace_name=deployment_name):
                logger.warning(f"Using default repo_name '{resolved_repo_name}' and default branch '{resolved_branch}' for Git trigger.")
            elif resolved_branch == zadig_config.default_git_branch:
                logger.warning(f"Using extracted repo_name '{extracted_repo_name}' and default branch '{resolved_branch}' for Git trigger.")
        else:
            resolved_repo_name = extracted_repo_name
            resolved_branch = extracted_branch

        # Assume service name is the same as deployment name for simplicity
        # This might need to be refined if Zadig service names differ from K8s deployment names
        service_name = deployment_name 

        # Add Git trigger
        trigger_added = await self.zadig_manager.add_git_trigger(
            service_name=service_name,
            repo_owner=zadig_config.repo_owner, # Use configured default
            repo_name=resolved_repo_name,
            branch=resolved_branch,
            events=zadig_config.git_trigger_events
        )

        if trigger_added:
            logger.info(f"Successfully added Git trigger for service {service_name} (Deployment: {deployment_name}) in env {namespace_name} with branch '{resolved_branch}'.")
        else:
            logger.error(f"Failed to add Git trigger for service {service_name} (Deployment: {deployment_name}) in env {namespace_name} with branch '{resolved_branch}'.")
        

    
    async def _on_namespace_updated(self, namespace, old_namespace):
        """Handle namespace update event"""
        # For now, we don't need to handle updates
        pass
    
    async def _on_namespace_deleted(self, namespace):
        """Handle namespace delete event"""
        namespace_name = namespace.metadata.name
        
        logger.info(f"Namespace deleted: {namespace_name}")
        
        # Create task for parallel processing
        task = asyncio.create_task(self.handle_namespace_deleted(namespace_name))
        self.namespace_tasks.add(task)
        task.add_done_callback(lambda t: self.namespace_tasks.discard(t))
    
    async def initialize(self):
        """Initialize the watcher with existing namespaces"""
        try:
            # Connect to Redis
            await self.state_manager.connect()
            
            # Get tracked namespaces from Redis
            redis_tracked = await self.state_manager.get_tracked_namespaces()
            logger.info(f"Found {len(redis_tracked)} namespaces in Redis state")
            
            # Note: The informer will handle initial list and reconciliation
            # through its add_func when it starts
            
        except Exception as e:
            logger.error(f"Failed to initialize: {e}")
            raise
    
    async def handle_namespace_created(self, namespace_name: str):
        """Handle namespace creation event"""
        logger.info(f"Processing namespace creation: {namespace_name}")
        
        # Mark as processing
        self.processing_namespaces.add(namespace_name)
        self.creation_tasks[namespace_name] = []
        
        try:
            # Create all resources
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
        # Group 1: Certificate first
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
        
        # Group 3: All other tasks can run in parallel
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
    
    async def reconcile_with_redis(self):
        """Reconcile current state with Redis"""
        try:
            # Get namespaces from informer cache
            cached_namespaces = self.informer.list()
            k8s_namespaces = set()
            
            for ns_name, ns in cached_namespaces.items():
                if self._should_track_namespace(ns):
                    k8s_namespaces.add(ns_name)
            
            # Get tracked namespaces from Redis
            redis_tracked = await self.state_manager.get_tracked_namespaces()
            
            # Find namespaces that need reconciliation
            namespaces_to_reconcile = k8s_namespaces - redis_tracked
            
            if namespaces_to_reconcile:
                logger.info(f"Found {len(namespaces_to_reconcile)} namespaces to reconcile with Redis")
                for ns_name in namespaces_to_reconcile:
                    # The informer will trigger add events for these
                    logger.info(f"Namespace {ns_name} needs reconciliation")
            
            # Check for namespaces in Redis but not in K8s
            deleted_namespaces = redis_tracked - k8s_namespaces
            for ns in deleted_namespaces:
                logger.warning(f"Namespace {ns} exists in Redis but not in K8s, marking as deleted")
                await self.state_manager.mark_namespace_deleted(ns)
                
        except Exception as e:
            logger.error(f"Failed to reconcile with Redis: {e}")
    
    async def run(self):
        """Run the namespace watcher"""
        try:
            await self.initialize()
            
            # Start reconciliation task
            reconcile_task = asyncio.create_task(self._periodic_reconciliation())
            
            # Start the informers
            await self.informer.start()
            await self.deployment_informer.start() # Start Deployment Informer
            
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            raise
        finally:
            await self.shutdown()
    
    async def _periodic_reconciliation(self):
        """Periodically reconcile with Redis"""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                await self.reconcile_with_redis()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic reconciliation: {e}")
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down namespace watcher")
        self._shutdown_event.set()
        self.informer.stop()
        self.deployment_informer.stop() # Stop Deployment Informer
        
        # Wait for all namespace tasks to complete
        if self.namespace_tasks:
            logger.info(f"Waiting for {len(self.namespace_tasks)} namespace tasks to complete")
            await asyncio.gather(*self.namespace_tasks, return_exceptions=True)
        
        # Disconnect from Redis
        await self.state_manager.disconnect()


def signal_handler(sig, _frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {sig}")
    asyncio.create_task(watcher.shutdown())
    sys.exit(0)


async def main():
    """Main entry point"""
    global watcher
    watcher = NamespaceWatcherV2()
    
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