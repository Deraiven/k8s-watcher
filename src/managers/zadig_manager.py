"""
Zadig workflow manager
"""
import json
from typing import Dict, List, Any, Optional
import aiohttp
import asyncio
import time
import datetime
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
        
        # 1. 获取所有环境信息 (这里假设你有一个获取列表的方法)
        # envs = await self.get_environments_from_zadig() 
        # 这里用你之前提供的 JSON 列表逻辑
        whiteList = ["test17", "test33", "test5", "test50", "test66"]
        envList = await self.get_environments()
        expiry_threshold = int(time.time()) - (15 * 24 * 60 * 60)
        
        for env in envList:
            env_name = env.get('env_key') # 根据实际字段取值
            update_time = env.get('update_time', 0)
            is_production = env.get('production', False)
            status = env.get("status") 

            # 3. 过滤逻辑：非生产环境 且 超过15天未更新
            if not is_production and update_time < expiry_threshold and status != "sleeping" and env_name not in whiteList:
                last_date = datetime.datetime.fromtimestamp(update_time).strftime('%Y-%m-%d')
                logger.info(f"🚩 检测到过期环境: {env_name} (最后更新时间: {last_date})")

                # 第二步：如果工作流更新成功（或容错），执行物理删除环境
                await self._clear_environment(env_name)
        
        logger.info("环境清理巡检完成。")
    
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
    
    