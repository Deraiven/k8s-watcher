"""
Zadig workflow manager
"""
import json
from typing import Dict, List, Any, Optional
import aiohttp
import asyncio
import time
import datetime
import re
from kubernetes import client
from kubernetes.client.rest import ApiException
from ..config.settings import zadig_config, app_config
from ..utils.logger import setup_logger
from ..utils.retry import async_retry
from ..utils.schedule import AdvancedScheduler

logger = setup_logger(__name__)


class ZadigManager:
    """Manager for Zadig workflow operations"""
    
    def __init__(self):
        self.base_url = zadig_config.url
        self.token = zadig_config.token
        self.project_key = zadig_config.project_key
        self.reference_env = app_config.reference_env
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        self.v1 = client.CoreV1Api()
    
    @staticmethod # 使用 classmethod 方便装饰器直接调用
    @AdvancedScheduler.daily(time_str="03:00", description="自动清理Zadig过期环境")
    def scheduled_cleanup_job():
        """调度器触发的入口函数"""
        manager = ZadigManager() # 实例化 manager
        # 因为调度器在独立线程运行，这里需要处理异步循环
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(manager.cleanup_expired_environments())
            loop.run_until_complete(manager.cleanup_orphaned_k8s_namespaces())
        finally:
            loop.close()


    async def get_environments(self):
        """获取项目下所有环境"""
        url = f"{self.base_url}/openapi/environments?projectKey={self.project_key}"
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=self.headers) as resp:
                    if resp.status != 200:
                        logger.error(f"获取环境列表失败: {resp.status}")
                        return
                    return await resp.json()  # 必须 await
            except Exception as e:
                logger.error(f"请求环境列表异常: {e}")
                raise
            
    async def cleanup_expired_environments(self):
        """执行具体的清理逻辑"""
        logger.info("开始执行环境清理巡检...")
        
        whiteList = ["test17", "test33", "test5", "test50"]
        envList = await self.get_environments()
        if not envList:
            logger.warning("环境清理巡检结束：未获取到环境列表")
            return
        now = int(time.time())
        expiry_threshold = now - (7 * 24 * 60 * 60)
        logger.info(
            "环境清理阈值: %s (7天前)",
            datetime.datetime.fromtimestamp(expiry_threshold).strftime('%Y-%m-%d %H:%M:%S')
        )
        total_count = len(envList)
        sleep_candidates = 0
        slept_count = 0
        delete_candidates = 0
        deleted_count = 0
        
        for env in envList:
            env_name = env.get('env_key') # 根据实际字段取值
            namespace_name = env.get('namespace') or env_name
            raw_update_time = env.get('update_time', 0)
            is_production = env.get('production', False)
            status = env.get("status") 
            if not env_name:
                continue
            if not self._is_managed_test_namespace(namespace_name):
                logger.debug(f"跳过非托管测试环境: {env_name} (namespace={namespace_name})")
                continue
            if env_name in whiteList:
                logger.debug(f"跳过白名单环境: {env_name}")
                continue
            if is_production:
                logger.debug(f"跳过生产环境: {env_name}")
                continue

            # Zadig 返回的 update_time 可能是毫秒级时间戳，统一转换到秒。
            update_time = self._normalize_timestamp(raw_update_time)

            if status == "sleeping":
                if update_time and update_time < expiry_threshold:
                    delete_candidates += 1
                    last_date = datetime.datetime.fromtimestamp(update_time).strftime('%Y-%m-%d')
                    logger.info(f"检测到睡眠超过7天环境: {env_name} (睡眠/更新时间: {last_date})")
                    if await self._clear_environment(env_name):
                        deleted_count += 1
                continue

            namespace_created_at = await self._get_namespace_created_timestamp(namespace_name)
            if namespace_created_at and namespace_created_at < expiry_threshold:
                sleep_candidates += 1
                created_date = datetime.datetime.fromtimestamp(namespace_created_at).strftime('%Y-%m-%d')
                logger.info(f"检测到创建超过7天环境: {env_name} (namespace={namespace_name}, 创建时间: {created_date})")
                if await self._set_environment_sleep(env_name, "enable"):
                    slept_count += 1
        
        logger.info(
            "环境清理巡检完成。总数=%s, 睡眠候选=%s, 实际睡眠=%s, 删除候选=%s, 实际删除=%s",
            total_count,
            sleep_candidates,
            slept_count,
            delete_candidates,
            deleted_count,
        )

    async def cleanup_orphaned_k8s_namespaces(self):
        """删除集群中存在但 Zadig fat 项目不存在的托管测试 namespace。"""
        logger.info("开始执行孤儿 namespace 清理巡检...")
        env_list = await self.get_environments()
        if env_list is None:
            logger.warning("孤儿 namespace 清理结束：未获取到 Zadig 环境列表")
            return

        zadig_namespaces = {
            env.get("namespace") or env.get("env_key")
            for env in env_list
            if env.get("namespace") or env.get("env_key")
        }

        deleted_count = 0
        skipped_count = 0
        namespaces = await asyncio.to_thread(self.v1.list_namespace)
        for namespace in namespaces.items:
            namespace_name = namespace.metadata.name
            labels = namespace.metadata.labels or {}

            if not self._is_managed_test_namespace(namespace_name):
                continue
            if labels.get(app_config.namespace_label_key) != app_config.namespace_label_value:
                skipped_count += 1
                logger.debug(f"跳过非 koderover 创建 namespace: {namespace_name}")
                continue
            if namespace_name in zadig_namespaces:
                continue

            logger.info(f"检测到孤儿 namespace: {namespace_name}，Zadig 项目 {self.project_key} 中不存在，准备删除")
            if await self._delete_k8s_namespace(namespace_name):
                deleted_count += 1

        logger.info(
            "孤儿 namespace 清理巡检完成。Zadig环境数=%s, 跳过=%s, 实际删除=%s",
            len(zadig_namespaces),
            skipped_count,
            deleted_count,
        )

    def _normalize_timestamp(self, value) -> int:
        try:
            timestamp = int(value or 0)
        except (TypeError, ValueError):
            return 0
        if timestamp > 10**12:
            timestamp = timestamp // 1000
        return timestamp

    def _is_managed_test_namespace(self, namespace_name: str) -> bool:
        return bool(re.fullmatch(r"test\d+", namespace_name or ""))

    async def _get_namespace_created_timestamp(self, namespace_name: str) -> int:
        try:
            namespace = await asyncio.to_thread(
                self.v1.read_namespace,
                name=namespace_name,
            )
            creation_time = namespace.metadata.creation_timestamp
            if not creation_time:
                return 0
            return int(creation_time.timestamp())
        except ApiException as e:
            if e.status == 404:
                logger.info(f"namespace {namespace_name} 不存在，跳过睡眠判断")
                return 0
            logger.error(f"查询 namespace {namespace_name} 创建时间失败: {e}")
            return 0
        except Exception as e:
            logger.error(f"查询 namespace {namespace_name} 创建时间异常: {e}")
            return 0

    async def _delete_k8s_namespace(self, namespace_name: str) -> bool:
        try:
            await asyncio.to_thread(
                self.v1.delete_namespace,
                name=namespace_name,
            )
            logger.info(f"删除孤儿 namespace {namespace_name} 成功")
            return True
        except ApiException as e:
            if e.status == 404:
                logger.info(f"namespace {namespace_name} 已不存在，跳过删除")
                return True
            logger.error(f"删除 namespace {namespace_name} 失败: {e}")
            return False
        except Exception as e:
            logger.error(f"删除 namespace {namespace_name} 异常: {e}")
            return False
    
    @async_retry(max_tries=3, exceptions=(aiohttp.ClientError,))
    async def _clear_environment(self, env):
        url = f"{self.base_url}/openapi/environments/{env}?projectKey={self.project_key}&isDelete=true"
        async with aiohttp.ClientSession() as session:
            try:
                async with session.delete(url, headers=self.headers) as resp:
                    if resp.status < 300:
                        logger.info(f"删除环境{env} 成功: {resp.status}")
                        return True
                    logger.error(f"删除环境{env} 失败: {resp.status}")
                    return False
            except Exception as e:
                logger.error(f"删除环境异常: {e}")
                raise

    @async_retry(max_tries=3, exceptions=(aiohttp.ClientError,))
    async def _set_environment_sleep(self, env: str, action: str) -> bool:
        """Set environment sleep status. action=enable sleeps, action=disable wakes."""
        url = f"{self.base_url}/api/aslan/environment/environments/{env}/sleep?projectName={self.project_key}&action={action}"
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, headers=self.headers) as resp:
                    if resp.status < 300:
                        logger.info(f"设置环境{env} sleep action={action} 成功: {resp.status}")
                        return True
                    error_text = await resp.text()
                    logger.error(f"设置环境{env} sleep action={action} 失败: {resp.status}, {error_text}")
                    return False
            except Exception as e:
                logger.error(f"设置环境 sleep 状态异常: {e}")
                raise

    @async_retry(max_tries=3, exceptions=(aiohttp.ClientError,))
    async def update_workflow_environments(self, action: str, env: str) -> bool:
        """Add or remove environment from workflow parameters"""
        async with aiohttp.ClientSession() as session:
            try:
                # Update backend workflow only
                backend_result = await self._update_backend_workflow(session, action, env)
                
                return backend_result
                
            except Exception as e:
                logger.error(f"Failed to update workflows: {e}")
                raise
    
    def fix_workflow_data(self, data):
    # 将脚本中的单反斜杠替换为双反斜杠，避开 YAML 转义检查
        if isinstance(data, dict):
            for k, v in data.items():
                if k == 'script' and isinstance(v, str):
                    # 核心修复：处理 \033 等转义序列
                    data[k] = v.replace('\\', '\\\\')
                else:
                    self.fix_workflow_data(v)
        elif isinstance(data, list):
            for item in data:
                self.fix_workflow_data(item)
        return data
    
    async def _update_backend_workflow(self, session: aiohttp.ClientSession, action: str, env: str) -> bool:
        """Update backend workflow (fat-pipelines)"""
        workflow_name = "fat-pipelines"
        
        # Get current workflow configuration
        url = f"{self.base_url}/openapi/workflows/custom/{workflow_name}/detail?projectKey={self.project_key}"
        
        async with session.get(url, headers=self.headers) as response:
            if response.status != 200:
                logger.error(f"Failed to get workflow {workflow_name}: {response.status}")
                return False
            
            data = await response.json()
        
        # Update environment parameter
        params_updated = False
        for param in data.get('params', []):
            if param.get('name') == 'cluster':
                choice_options = param.get('choice_option', [])
                
                if action == 'add' and env not in choice_options:
                    choice_options.append(env)
                    params_updated = True
                    logger.info(f"Added {env} to workflow parameters")
                elif action == 'delete' and env in choice_options:
                    choice_options.remove(env)
                    params_updated = True
                    logger.info(f"Removed {env} from workflow parameters")
                
                param['choice_option'] = choice_options
        
        # Update Apollo configuration stage if deleting
        # if action == 'delete':
        #     stages = data.get('stages', [])
        #     for stage in stages:
        #         if stage.get('name') == '配置检查':
        #             for job in stage.get('jobs', []):
        #                 if job.get('name') == '配置变更':
        #                     namespace_list = job.get('spec', {}).get('namespaceList', [])
        #                     job['spec']['namespaceList'] = [
        #                         ns for ns in namespace_list 
        #                         if ns.get('clusterID') != env
        #                     ]
        
        # Save updated workflow
        if params_updated or action == 'delete':
            
            # 2. 将 stages 对象先转成 JSON 字符串，把所有单个反斜杠变成双反斜杠
# 这样发送过去后，Zadig 存入 YAML 的就是原始字符 '\033' 而不是转义后的控制符
            # fixed_stages = self.fix_workflow_data(stages)
            update_data = {
                "name": workflow_name,
                "display_name": data.get('display_name', 'fat-pipelines'),
                "concurrency_limit": data.get('concurrency_limit', 10),
                "project": self.project_key,
                "params": data.get('params', []),
                "stages": data.get('stages', [])
            }
            
            update_url = f"{self.base_url}/api/aslan/workflow/v4/{workflow_name}?projectName={self.project_key}"
            
            async with session.put(update_url, headers=self.headers, data=json.dumps(update_data, ensure_ascii=False)) as response:
                if response.status >= 200 and response.status < 300:
                    logger.info(f"Updated backend workflow {workflow_name}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to update workflow: {error_text}")
                    return False
        
        return True
    
    
