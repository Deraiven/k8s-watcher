"""
Redis-based state management for namespace tracking
"""
import json
import asyncio
from typing import Set, Dict, Optional, List
from datetime import datetime
import redis.asyncio as redis

from ..config.settings import redis_config
from ..utils.logger import setup_logger

logger = setup_logger(__name__)


class RedisStateManager:
    """Manage persistent state using Redis"""
    
    def __init__(self):
        self.redis_url = redis_config.url
        self.key_prefix = redis_config.key_prefix
        self.ttl_days = redis_config.ttl_days
        self._redis: Optional[redis.Redis] = None
    
    async def connect(self):
        """Connect to Redis"""
        try:
            self._redis = redis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True
            )
            # Test connection
            await self._redis.ping()
            logger.info("Connected to Redis")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from Redis"""
        if self._redis:
            await self._redis.close()
            logger.info("Disconnected from Redis")
    
    def _get_namespace_key(self, namespace: str) -> str:
        """Get Redis key for namespace"""
        return f"{self.key_prefix}:namespace:{namespace}"
    
    def _get_resource_key(self, namespace: str, resource_type: str) -> str:
        """Get Redis key for namespace resources"""
        return f"{self.key_prefix}:resources:{namespace}:{resource_type}"
    
    async def mark_namespace_created(self, namespace: str):
        """Mark namespace as created with metadata"""
        key = self._get_namespace_key(namespace)
        data = {
            "status": "created",
            "created_at": datetime.utcnow().isoformat(),
            "last_reconciled": datetime.utcnow().isoformat()
        }
        
        await self._redis.hset(key, mapping=data)
        # Set TTL for automatic cleanup (optional)
        # await self._redis.expire(key, self.ttl_days * 24 * 60 * 60)
        
        # Add to active namespaces set
        await self._redis.sadd(f"{self.key_prefix}:active_namespaces", namespace)
        logger.debug(f"Marked namespace {namespace} as created")
    
    async def mark_namespace_deleted(self, namespace: str):
        """Mark namespace as deleted"""
        key = self._get_namespace_key(namespace)
        
        # Update status
        await self._redis.hset(key, mapping={
            "status": "deleted",
            "deleted_at": datetime.utcnow().isoformat()
        })
        
        # Remove from active set, add to deleted set
        await self._redis.srem(f"{self.key_prefix}:active_namespaces", namespace)
        await self._redis.sadd(f"{self.key_prefix}:deleted_namespaces", namespace)
        
        # Set shorter TTL for deleted namespaces
        await self._redis.expire(key, self.ttl_days * 24 * 60 * 60)
        logger.debug(f"Marked namespace {namespace} as deleted")
    
    async def mark_namespace_reconciled(self, namespace: str):
        """Update reconciliation timestamp"""
        key = self._get_namespace_key(namespace)
        await self._redis.hset(key, "last_reconciled", datetime.utcnow().isoformat())
    
    async def record_resource_created(self, namespace: str, resource_type: str, resource_id: str):
        """Record that a resource was created for a namespace"""
        key = self._get_resource_key(namespace, resource_type)
        await self._redis.sadd(key, resource_id)
        logger.debug(f"Recorded {resource_type} resource {resource_id} for {namespace}")
    
    async def record_resource_deleted(self, namespace: str, resource_type: str, resource_id: str):
        """Record that a resource was deleted"""
        key = self._get_resource_key(namespace, resource_type)
        await self._redis.srem(key, resource_id)
    
    async def get_namespace_resources(self, namespace: str, resource_type: str) -> Set[str]:
        """Get all resources of a type for a namespace"""
        key = self._get_resource_key(namespace, resource_type)
        return set(await self._redis.smembers(key))
    
    async def is_namespace_tracked(self, namespace: str) -> bool:
        """Check if namespace is actively tracked"""
        return await self._redis.sismember(f"{self.key_prefix}:active_namespaces", namespace)
    
    async def get_tracked_namespaces(self) -> Set[str]:
        """Get all actively tracked namespaces"""
        return set(await self._redis.smembers(f"{self.key_prefix}:active_namespaces"))
    
    async def get_deleted_namespaces(self) -> Set[str]:
        """Get all deleted namespaces"""
        return set(await self._redis.smembers(f"{self.key_prefix}:deleted_namespaces"))
    
    async def get_namespace_info(self, namespace: str) -> Optional[Dict]:
        """Get all information about a namespace"""
        key = self._get_namespace_key(namespace)
        data = await self._redis.hgetall(key)
        return data if data else None
    
    async def get_namespaces_needing_reconciliation(self, hours: int = 24) -> List[str]:
        """Get namespaces that haven't been reconciled recently"""
        cutoff = datetime.utcnow().timestamp() - (hours * 60 * 60)
        namespaces_to_check = []
        
        active_namespaces = await self.get_tracked_namespaces()
        for ns in active_namespaces:
            info = await self.get_namespace_info(ns)
            if info and "last_reconciled" in info:
                last_reconciled = datetime.fromisoformat(info["last_reconciled"]).timestamp()
                if last_reconciled < cutoff:
                    namespaces_to_check.append(ns)
            else:
                # No reconciliation record, needs checking
                namespaces_to_check.append(ns)
        
        return namespaces_to_check
    
    async def find_orphaned_resources(self) -> Dict[str, List[str]]:
        """Find resources for deleted namespaces"""
        orphaned = {}
        
        deleted_namespaces = await self.get_deleted_namespaces()
        for ns in deleted_namespaces:
            # Check each resource type
            for resource_type in ["dns", "certificate", "sqs", "sns", "kong_route"]:
                resources = await self.get_namespace_resources(ns, resource_type)
                if resources:
                    if ns not in orphaned:
                        orphaned[ns] = []
                    orphaned[ns].extend([f"{resource_type}:{r}" for r in resources])
        
        return orphaned
    
    async def cleanup_namespace_data(self, namespace: str):
        """Remove all data for a namespace"""
        # Delete namespace key
        await self._redis.delete(self._get_namespace_key(namespace))
        
        # Delete resource keys
        for resource_type in ["dns", "certificate", "sqs", "sns", "kong_route"]:
            await self._redis.delete(self._get_resource_key(namespace, resource_type))
        
        # Remove from sets
        await self._redis.srem(f"{self.key_prefix}:deleted_namespaces", namespace)
        await self._redis.srem(f"{self.key_prefix}:active_namespaces", namespace)
        
        logger.info(f"Cleaned up all data for namespace {namespace}")