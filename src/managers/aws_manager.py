"""
AWS resources manager for SQS and SNS
"""
import json
import re
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

    def _matches_env_token(self, text: str, env: str) -> bool:
        """
        Match environment as a standalone token split by non-alnum delimiters.
        Example: test1 must not match test10/test12.
        """
        if not text:
            return False
        env_token = (env or "").strip().lower()
        if not env_token:
            return False
        tokens = [tok for tok in re.split(r"[^A-Za-z0-9]+", text.lower()) if tok]
        return env_token in tokens

    async def _list_queue_urls(self, sqs) -> List[str]:
        """List all SQS queue URLs with explicit pagination."""
        queue_urls = []
        next_token = None
        while True:
            params = {"MaxResults": 1000}
            if next_token:
                params["NextToken"] = next_token
            response = await sqs.list_queues(**params)
            queue_urls.extend(response.get("QueueUrls", []))
            next_token = response.get("NextToken")
            if not next_token:
                break
        return queue_urls

    def _build_queue_attributes(self, attributes: Dict[str, Any], env: str) -> Dict[str, str]:
        """Copy queue attributes accepted by CreateQueue."""
        copy_keys = [
            "DelaySeconds",
            "MaximumMessageSize",
            "MessageRetentionPeriod",
            "Policy",
            "ReceiveMessageWaitTimeSeconds",
            "RedriveAllowPolicy",
            "RedrivePolicy",
            "VisibilityTimeout",
            "KmsMasterKeyId",
            "KmsDataKeyReusePeriodSeconds",
            "SqsManagedSseEnabled",
            "FifoQueue",
            "ContentBasedDeduplication",
            "DeduplicationScope",
            "FifoThroughputLimit",
        ]
        new_attributes = {}
        for key in copy_keys:
            value = attributes.get(key)
            if value is None:
                continue
            if key in ("Policy", "RedrivePolicy", "RedriveAllowPolicy"):
                value = value.replace(self.reference_env.upper(), env.upper()).replace(self.reference_env, env)
            new_attributes[key] = value
        return new_attributes
    
    @async_retry(max_tries=3, exceptions=(ClientError,))
    async def create_sqs_queues(self, env: str) -> List[str]:
        """Create SQS queues based on reference environment"""
        created_queues = []

        async with self.session.client('sqs') as sqs:
            queue_urls = await self._list_queue_urls(sqs)
            existing_queues = {
                queue_url.split("/")[-1]
                for queue_url in queue_urls
                if self._matches_env_token(queue_url.split("/")[-1], env)
            }
            reference_queue_urls = [
                queue_url for queue_url in queue_urls
                if self._matches_env_token(queue_url.split("/")[-1], self.reference_env)
            ]
            logger.info(
                "Found %s reference SQS queues for %s when creating %s",
                len(reference_queue_urls),
                self.reference_env,
                env,
            )

            for queue_url in reference_queue_urls:
                source_queue_name = queue_url.split("/")[-1]
                try:
                    attrs_response = await sqs.get_queue_attributes(
                        QueueUrl=queue_url,
                        AttributeNames=["All"],
                    )
                    attributes = attrs_response.get("Attributes", {})
                    new_attributes = self._build_queue_attributes(attributes, env)
                    queue_name = source_queue_name.replace(
                        self.reference_env.upper(), env.upper()
                    ).replace(self.reference_env, env)

                    if queue_name in existing_queues:
                        logger.info(f"Queue already exists: {queue_name}, skipping")
                        created_queues.append(queue_name)
                        continue

                    try:
                        await sqs.create_queue(
                            QueueName=queue_name,
                            Attributes=new_attributes,
                        )
                        created_queues.append(queue_name)
                        logger.info(f"Created SQS queue: {queue_name}")
                    except ClientError as e:
                        if e.response['Error']['Code'] == 'QueueAlreadyExists':
                            logger.info(f"Queue already exists: {queue_name}, skipping")
                            created_queues.append(queue_name)
                        else:
                            logger.error(f"Failed to create queue {queue_name}: {e}")
                            raise

                except Exception as e:
                    logger.error(f"Failed to process queue {source_queue_name}: {e}")
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
                if self._matches_env_token(queue_name, env):
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
