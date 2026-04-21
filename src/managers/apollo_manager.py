"""
Apollo configuration manager
"""
import json
import time
from typing import List, Dict, Optional, Tuple
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
        self._deployment_aliases = {
            "backoffice-v1-web-app": "backoffice-v2-webapp",
            "beep-v1-web": "beep-v1-webapp",
            "online-purchase-svc-cronjob": "online-purchase-svc",
        }
        self._skip_apps = {"bo-v1-assets", "inventory-cronjob"}
    
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

    def _normalize_app_id(self, deployment_name: str) -> str:
        app_id = self._deployment_aliases.get(deployment_name, deployment_name)
        if app_id.startswith("backoffice-v1-web"):
            return "backoffice-v1-web"
        return app_id

    def _resolve_target_app_ids(self, deployment_name: str) -> List[str]:
        # For backoffice web app, keep both legacy and v2 app configs in sync.
        if deployment_name == "backoffice-v1-web-app":
            return ["backoffice-v2-webapp", "backoffice-v1-web"]
        # For beep web app, keep both legacy and webapp configs in sync.
        if deployment_name == "beep-v1-web":
            return ["beep-v1-web", "beep-v1-webapp"]
        return [self._normalize_app_id(deployment_name)]

    async def ensure_subenv_app_config(self, deployment_name: str, env: str) -> bool:
        """Ensure app configuration for a deployment exists in the target environment cluster."""
        target_app_ids = self._resolve_target_app_ids(deployment_name)
        synced_count = 0
        for app_id in target_app_ids:
            if app_id in self._skip_apps:
                logger.info(f"Skip Apollo sync for app {app_id}")
                continue
            ok = await self._sync_single_app_config(app_id, env)
            if ok:
                synced_count += 1

        if synced_count == 0:
            logger.warning(
                "No Apollo app config synced for deployment=%s env=%s targets=%s",
                deployment_name,
                env,
                target_app_ids,
            )
            return False
        return True

    async def _sync_single_app_config(self, app_id: str, env: str) -> bool:
        conn = None
        cursor = None
        try:
            conn = await self._get_connection()
            cursor = await conn.cursor()
            await conn.begin()

            await cursor.execute(
                """
                SELECT COUNT(*)
                FROM Cluster
                WHERE Name=%s AND AppId=%s AND IsDeleted=0
                """,
                (env, app_id),
            )
            result = await cursor.fetchone()
            if result and result[0] > 0:
                await conn.rollback()
                logger.info(f"Apollo app config already exists for app={app_id} env={env}")
                return True

            await cursor.execute(
                """
                INSERT INTO Cluster
                    (Name, AppId, ParentClusterId, IsDeleted, DataChange_CreatedBy, DataChange_CreatedTime, DataChange_LastModifiedBy, DataChange_LastTime)
                SELECT
                    %s, AppId, ParentClusterId, IsDeleted, DataChange_CreatedBy, DataChange_CreatedTime, DataChange_LastModifiedBy, DataChange_LastTime
                FROM Cluster
                WHERE Name=%s AND IsDeleted=0 AND AppId=%s
                """,
                (env, self.reference_env, app_id),
            )
            if cursor.rowcount == 0:
                await conn.rollback()
                logger.warning(
                    "No template Cluster found in reference env for app=%s ref_env=%s",
                    app_id,
                    self.reference_env,
                )
                return False

            await cursor.execute(
                """
                INSERT INTO Namespace
                    (AppId, ClusterName, NamespaceName, IsDeleted, DataChange_CreatedBy, DataChange_CreatedTime, DataChange_LastModifiedBy, DataChange_LastTime)
                SELECT
                    AppId, %s, NamespaceName, IsDeleted, DataChange_CreatedBy, DataChange_CreatedTime, DataChange_LastModifiedBy, DataChange_LastTime
                FROM Namespace
                WHERE ClusterName=%s AND AppId=%s
                """,
                (env, self.reference_env, app_id),
            )

            cm_ok, cm_configurations = await self._clone_namespace_items(
                cursor=cursor,
                app_id=app_id,
                env=env,
                namespace_name=f"web.{app_id}",
            )
            secret_ok, secret_configurations = await self._clone_namespace_items(
                cursor=cursor,
                app_id=app_id,
                env=env,
                namespace_name="secret",
            )

            release_rows = []
            if cm_ok:
                release_rows.append(
                    (
                        f"AUTO-{int(time.time() * 1000)}-cm",
                        "release",
                        app_id,
                        env,
                        f"web.{app_id}",
                        json.dumps(cm_configurations, ensure_ascii=False),
                    )
                )
            if secret_ok:
                release_rows.append(
                    (
                        f"AUTO-{int(time.time() * 1000)}-secret",
                        "release",
                        app_id,
                        env,
                        "secret",
                        json.dumps(secret_configurations, ensure_ascii=False),
                    )
                )
            if release_rows:
                await cursor.executemany(
                    """
                    INSERT INTO `Release` (ReleaseKey, Name, AppId, ClusterName, NamespaceName, Configurations)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    release_rows,
                )

            await conn.commit()
            logger.info(f"Apollo app config synced for app={app_id} env={env}")
            return True
        except Exception as e:
            if conn:
                await conn.rollback()
            logger.error(f"Failed to sync Apollo app config for app={app_id} env={env}: {e}")
            raise
        finally:
            if cursor:
                await cursor.close()
            if conn:
                conn.close()

    async def _clone_namespace_items(
        self,
        cursor,
        app_id: str,
        env: str,
        namespace_name: str,
    ) -> Tuple[bool, Dict[str, str]]:
        """Clone Item rows from reference cluster namespace to target cluster namespace."""
        await cursor.execute(
            """
            SELECT Id
            FROM Namespace
            WHERE IsDeleted=0 AND AppId=%s AND ClusterName=%s AND NamespaceName=%s
            LIMIT 1
            """,
            (app_id, self.reference_env, namespace_name),
        )
        src = await cursor.fetchone()
        if not src:
            return False, {}
        src_namespace_id = src[0]

        await cursor.execute(
            """
            SELECT Id
            FROM Namespace
            WHERE IsDeleted=0 AND AppId=%s AND ClusterName=%s AND NamespaceName=%s
            LIMIT 1
            """,
            (app_id, env, namespace_name),
        )
        dst = await cursor.fetchone()
        if not dst:
            return False, {}
        dst_namespace_id = dst[0]

        await cursor.execute(
            """
            SELECT Item.`Key`, Item.Value, Item.Comment
            FROM Item
            WHERE Item.NamespaceId=%s AND Item.IsDeleted=0 AND LENGTH(Item.`Key`) != 0
            """,
            (src_namespace_id,),
        )
        rows = await cursor.fetchall()
        if not rows:
            return True, {}

        insert_rows = []
        configurations: Dict[str, str] = {}
        for key, value, comment in rows:
            normalized_value = value
            # Keep AWS resource references untouched, align env-specific values otherwise.
            if isinstance(normalized_value, str):
                if (
                    "https://sqs.ap-southeast-1.amazonaws.com/" not in normalized_value
                    and "arn:aws:sns:ap-southeast-1:" not in normalized_value
                ):
                    normalized_value = normalized_value.replace(self.reference_env.upper(), env.upper())
                    normalized_value = normalized_value.replace(self.reference_env, env)

            insert_rows.append(
                (dst_namespace_id, key, normalized_value, comment, "namespace-watcher", "namespace-watcher")
            )
            configurations[key] = normalized_value

        await cursor.executemany(
            """
            INSERT INTO Item
                (NamespaceId, `Key`, Value, Comment, DataChange_CreatedBy, DataChange_LastModifiedBy)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            insert_rows,
        )
        return True, configurations
