"""
Apollo configuration manager
"""
from typing import List, Dict, Optional
import aiomysql

from ..config.settings import db_config, app_config
from ..utils.logger import setup_logger
from ..utils.retry import async_retry

logger = setup_logger(__name__)


class ApolloManager:
    """Manager for Apollo configuration database operations"""
    
    def __init__(self):
        self.db_config = {
            'host': db_config.host,
            'port': db_config.port,
            'user': db_config.user,
            'password': db_config.password,
            'db': db_config.database
        }
        self.reference_env = app_config.reference_env
    
    async def _get_connection(self):
        """Get database connection"""
        return await aiomysql.connect(**self.db_config)
    
    @async_retry(max_tries=3)
    async def create_cluster_config(self, env: str) -> bool:
        """Create Apollo cluster configuration by copying from reference environment"""
        conn = None
        cursor = None
        
        try:
            conn = await self._get_connection()
            cursor = await conn.cursor()
            
            # Check if cluster already exists
            await cursor.execute(
                "SELECT COUNT(*) FROM Cluster WHERE Name = %s",
                (env,)
            )
            result = await cursor.fetchone()
            
            if result[0] > 0:
                logger.warning(f"Apollo cluster {env} already exists")
                return True
            
            # Begin transaction
            await conn.begin()
            
            # 1. Copy cluster
            await cursor.execute("""
                INSERT INTO Cluster (Name, AppId, IsDeleted, DataChange_CreatedBy, DataChange_CreatedTime)
                SELECT %s, AppId, IsDeleted, 'namespace-watcher', NOW()
                FROM Cluster
                WHERE Name = %s
            """, (env, self.reference_env))
            
            # 2. Copy namespaces
            # await cursor.execute("""
            #     INSERT INTO Namespace (AppId, ClusterName, NamespaceName, IsDeleted, DataChange_CreatedBy, DataChange_CreatedTime)
            #     SELECT AppId, %s, NamespaceName, IsDeleted, 'namespace-watcher', NOW()
            #     FROM Namespace
            #     WHERE ClusterName = %s
            # """, (env, self.reference_env))
            
            # 3. Copy items (configuration values)
            # await cursor.execute("""
            #     INSERT INTO Item (NamespaceId, Key, Type, Value, Comment, LineNum, IsDeleted, DataChange_CreatedBy, DataChange_CreatedTime)
            #     SELECT 
            #         n2.Id,
            #         i.Key,
            #         i.Type,
            #         i.Value,
            #         i.Comment,
            #         i.LineNum,
            #         i.IsDeleted,
            #         'namespace-watcher',
            #         NOW()
            #     FROM Item i
            #     JOIN Namespace n1 ON i.NamespaceId = n1.Id
            #     JOIN Namespace n2 ON n1.AppId = n2.AppId AND n1.NamespaceName = n2.NamespaceName
            #     WHERE n1.ClusterName = %s AND n2.ClusterName = %s
            # """, (self.reference_env, env))
            
            # 4. Create initial release
            # await cursor.execute("""
            #     INSERT INTO `Release` (AppId, ClusterName, NamespaceName, Name, Configurations, Comment, IsDeleted, DataChange_CreatedBy, DataChange_CreatedTime)
            #     SELECT 
            #         AppId, 
            #         %s,
            #         NamespaceName,
            #         CONCAT('Initial release for ', %s),
            #         Configurations,
            #         'Created by namespace-watcher',
            #         IsDeleted,
            #         'namespace-watcher',
            #         NOW()
            #     FROM `Release`
            #     WHERE ClusterName = %s AND IsDeleted = 0
            #     GROUP BY AppId, NamespaceName
            # """, (env, env, self.reference_env))
            
            # Commit transaction
            await conn.commit()
            
            logger.info(f"Successfully created Apollo configuration for {env}")
            return True
            
        except Exception as e:
            if conn:
                await conn.rollback()
            logger.error(f"Failed to create Apollo config for {env}: {e}")
            raise
        finally:
            if cursor:
                await cursor.close()
            if conn:
                conn.close()
    
    @async_retry(max_tries=3)
    async def delete_cluster_config(self, env: str) -> bool:
        """Delete Apollo cluster configuration"""
        conn = None
        cursor = None
        
        try:
            conn = await self._get_connection()
            cursor = await conn.cursor()
            
            # Begin transaction
            await conn.begin()
            
            # 1. Delete releases
            await cursor.execute(
                "DELETE FROM `Release` WHERE ClusterName = %s",
                (env,)
            )
            
            # 2. Delete items
            await cursor.execute("""
                DELETE FROM Item 
                WHERE NamespaceId IN (
                    SELECT Id FROM Namespace WHERE ClusterName = %s
                )
            """, (env,))
            
            # 3. Delete namespaces
            await cursor.execute(
                "DELETE FROM Namespace WHERE ClusterName = %s",
                (env,)
            )
            
            # 4. Delete cluster
            await cursor.execute(
                "DELETE FROM Cluster WHERE Name = %s",
                (env,)
            )
            
            # Commit transaction
            await conn.commit()
            
            logger.info(f"Successfully deleted Apollo configuration for {env}")
            return True
            
        except Exception as e:
            if conn:
                await conn.rollback()
            logger.error(f"Failed to delete Apollo config for {env}: {e}")
            raise
        finally:
            if cursor:
                await cursor.close()
            if conn:
                conn.close()
    
    async def get_cluster_apps(self, env: str) -> List[Dict[str, str]]:
        """Get all apps configured for a cluster"""
        conn = None
        cursor = None
        
        try:
            conn = await self._get_connection()
            cursor = await conn.cursor(aiomysql.DictCursor)
            
            await cursor.execute("""
                SELECT DISTINCT AppId, NamespaceName 
                FROM Namespace 
                WHERE ClusterName = %s AND IsDeleted = 0
            """, (env,))
            
            results = await cursor.fetchall()
            return results
            
        except Exception as e:
            logger.error(f"Failed to get cluster apps: {e}")
            return []
        finally:
            if cursor:
                await cursor.close()
            if conn:
                conn.close()