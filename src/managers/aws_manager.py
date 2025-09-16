"""
AWS resources manager for SQS and SNS
"""
import json
from typing import List, Dict, Any, Optional
import aioboto3
from botocore.exceptions import ClientError

from ..config.settings import aws_config, app_config
from ..utils.logger import setup_logger
from ..utils.retry import async_retry

logger = setup_logger(__name__)


class AWSManager:
    """Manager for AWS SQS and SNS resources"""
    
    def __init__(self):
        self.session = aioboto3.Session(
            aws_access_key_id=aws_config.access_key_id,
            aws_secret_access_key=aws_config.secret_access_key,
            region_name=aws_config.region
        )
        self.reference_env = app_config.reference_env
        self.account_id = None
    
    async def _get_account_id(self) -> str:
        """Get AWS account ID"""
        if not self.account_id:
            async with self.session.client('sts') as sts:
                response = await sts.get_caller_identity()
                self.account_id = response['Account']
        return self.account_id
    
    @async_retry(max_tries=3, exceptions=(ClientError,))
    async def create_sqs_queues(self, env: str) -> List[str]:
        """Create SQS queues based on reference environment"""
        created_queues = []
        
        async with self.session.resource('sqs') as sqs:
            # First, get list of existing queues for this environment
            existing_queues = set()
            async for existing_queue in sqs.queues.all():
                if env in existing_queue.url or env.upper() in existing_queue.url:
                    queue_name = existing_queue.url.split("/")[-1]
                    existing_queues.add(queue_name)
            # Get all queues
            async for queue in sqs.queues.all():
                if self.reference_env in queue.url or self.reference_env.upper() in queue.url:
                    try:
                        # Get queue attributes
                        attributes = await queue.attributes
                        
                        # Prepare new queue attributes
                        new_attributes = {}
                        if attributes.get("FifoQueue"):
                            new_attributes["FifoQueue"] = attributes["FifoQueue"]
                            new_attributes["FifoThroughputLimit"] = attributes.get("FifoThroughputLimit", "perQueue")
                            new_attributes["ContentBasedDeduplication"] = attributes.get("ContentBasedDeduplication", "false")
                            new_attributes["DeduplicationScope"] = attributes.get("DeduplicationScope", "queue")
                        
                        # Update policy if exists
                        if "Policy" in attributes:
                            new_attributes["Policy"] = attributes["Policy"].replace(
                                self.reference_env.upper(), env.upper()
                            ).replace(self.reference_env, env)
                        
                        # Create new queue name
                        queue_name = queue.url.split("/")[-1].replace(
                            self.reference_env.upper(), env.upper()
                        ).replace(self.reference_env, env)
                        
                        # Check if queue already exists
                        if queue_name in existing_queues:
                            logger.info(f"Queue already exists: {queue_name}, skipping")
                            created_queues.append(queue_name)
                            continue
                        
                        try:
                            # Create the queue
                            await sqs.create_queue(
                                QueueName=queue_name,
                                Attributes=new_attributes
                            )
                            created_queues.append(queue_name)
                            logger.info(f"Created SQS queue: {queue_name}")
                            
                        except ClientError as e:
                            if e.response['Error']['Code'] == 'QueueAlreadyExists':
                                logger.info(f"Queue already exists: {queue_name}, skipping")
                                created_queues.append(queue_name)  # Still track it as handled
                            else:
                                logger.error(f"Failed to create queue: {e}")
                                raise
                        
                    except Exception as e:
                        logger.error(f"Failed to process queue {queue.url}: {e}")
                        raise
        
        return created_queues
    
    @async_retry(max_tries=3, exceptions=(ClientError,))
    async def delete_sqs_queues(self, env: str) -> List[str]:
        """Delete SQS queues for the environment"""
        deleted_queues = []
        
        async with self.session.client('sqs') as sqs:
            # List all queues
            response = await sqs.list_queues()
            queue_urls = response.get('QueueUrls', [])
            
            # Filter and delete queues for this environment
            for queue_url in queue_urls:
                queue_name = queue_url.split("/")[-1]
                if env in queue_name or env.upper() in queue_name:
                    try:
                        await sqs.delete_queue(QueueUrl=queue_url)
                        deleted_queues.append(queue_name)
                        logger.info(f"Deleted SQS queue: {queue_name}")
                    except ClientError as e:
                        if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
                            logger.debug(f"Queue does not exist, skipping: {queue_name}")
                        else:
                            logger.error(f"Failed to delete queue {queue_name}: {e}")
        
        return deleted_queues
    
    @async_retry(max_tries=3, exceptions=(ClientError,))
    async def create_sns_topic(self, env: str) -> Dict[str, Any]:
        """Create SNS topic and subscriptions"""
        topic_name = f"notification_{env}"
        account_id = await self._get_account_id()
        
        async with self.session.client('sns') as sns:
            # Create topic (idempotent - will return existing topic if exists)
            response = await sns.create_topic(Name=topic_name)
            topic_arn = response['TopicArn']
            logger.info(f"Created/Retrieved SNS topic: {topic_name}")
            
            # Copy subscriptions from reference environment
            reference_topic_arn = f"arn:aws:sns:{aws_config.region}:{account_id}:notification_{self.reference_env}"
            
            try:
                # Get subscriptions from reference topic
                response = await sns.list_subscriptions_by_topic(
                    TopicArn=reference_topic_arn
                )
                
                subscriptions = []
                for sub in response['Subscriptions']:
                    if sub['Protocol'] == 'sqs':
                        # Update endpoint for new environment
                        new_endpoint = sub['Endpoint'].replace(
                            self.reference_env, env
                        ).replace(self.reference_env.upper(), env.upper())
                        
                        # Get subscription attributes
                        attrs_response = await sns.get_subscription_attributes(
                            SubscriptionArn=sub['SubscriptionArn']
                        )
                        filter_policy = attrs_response['Attributes'].get('FilterPolicy', '{}')
                        
                        try:
                            # Check if subscription already exists
                            existing_subs = await sns.list_subscriptions_by_topic(TopicArn=topic_arn)
                            existing_endpoints = [s['Endpoint'] for s in existing_subs.get('Subscriptions', [])]
                            
                            if new_endpoint in existing_endpoints:
                                logger.info(f"Subscription already exists for endpoint: {new_endpoint}, skipping")
                                # Find the existing subscription ARN
                                for existing_sub in existing_subs['Subscriptions']:
                                    if existing_sub['Endpoint'] == new_endpoint:
                                        subscriptions.append(existing_sub['SubscriptionArn'])
                                        break
                            else:
                                # Create new subscription
                                sub_response = await sns.subscribe(
                                    TopicArn=topic_arn,
                                    Protocol=sub['Protocol'],
                                    Endpoint=new_endpoint,
                                    Attributes={
                                        'FilterPolicy': filter_policy
                                    },
                                    ReturnSubscriptionArn=True
                                )
                                subscriptions.append(sub_response['SubscriptionArn'])
                                logger.info(f"Created SNS subscription: {new_endpoint}")
                                
                        except ClientError as e:
                            if 'already exists' in str(e):
                                logger.info(f"Subscription already exists: {new_endpoint}, skipping")
                            else:
                                logger.error(f"Failed to create subscription for {new_endpoint}: {e}")
                
                return {
                    'topic_arn': topic_arn,
                    'subscriptions': subscriptions
                }
                
            except ClientError as e:
                logger.error(f"Failed to copy subscriptions: {e}")
                return {
                    'topic_arn': topic_arn,
                    'subscriptions': []
                }
    
    @async_retry(max_tries=3, exceptions=(ClientError,))
    async def delete_sns_topic(self, env: str) -> bool:
        """Delete SNS topic and its subscriptions"""
        topic_name = f"notification_{env}"
        account_id = await self._get_account_id()
        topic_arn = f"arn:aws:sns:{aws_config.region}:{account_id}:{topic_name}"
        
        async with self.session.client('sns') as sns:
            try:
                # List and delete all subscriptions
                response = await sns.list_subscriptions_by_topic(TopicArn=topic_arn)
                for sub in response['Subscriptions']:
                    await sns.unsubscribe(SubscriptionArn=sub['SubscriptionArn'])
                    logger.info(f"Deleted SNS subscription: {sub['SubscriptionArn']}")
                
                # Delete the topic
                await sns.delete_topic(TopicArn=topic_arn)
                logger.info(f"Deleted SNS topic: {topic_name}")
                return True
                
            except ClientError as e:
                if e.response['Error']['Code'] in ['NotFound', 'NotFoundException']:
                    logger.debug(f"Topic not found, skipping: {topic_name}")
                else:
                    logger.error(f"Failed to delete topic: {e}")
                return False