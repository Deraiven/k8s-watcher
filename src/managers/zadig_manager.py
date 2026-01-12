"""
Zadig workflow manager
"""
import json
from typing import Dict, List, Any, Optional
import aiohttp

from ..config.settings import zadig_config, app_config
from ..utils.logger import setup_logger
from ..utils.retry import async_retry

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
            update_data = {
                "name": workflow_name,
                "project": self.project_key,
                "display_name": data.get('display_name', 'fat-pipelines'),
                "concurrency_limit": data.get('concurrency_limit', 10),
                "project": self.project_key,
                "params": data.get('params', []),
                "stages": data.get('stages', [])
            }
            
            update_url = f"{self.base_url}/api/aslan/workflow/v4/{workflow_name}?projectName={self.project_key}"
            
            async with session.put(update_url, headers=self.headers, data=json.dumps(update_data)) as response:
                if response.status >= 200 and response.status < 300:
                    logger.info(f"Updated backend workflow {workflow_name}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to update workflow: {error_text}")
                    return False
        
        return True
    
    # Frontend workflow update removed - no longer needed
    # The _update_frontend_workflow method has been removed as frontend-service workflow update is no longer required
    
    # @async_retry(max_tries=3, exceptions=(aiohttp.ClientError,))
    # async def add_app_to_workflow(self, app: str, env: str) -> bool:
    #     """Add application configuration to workflow"""
    #     if app == "bo-v1-assets" or app.startswith("backoffice-v1-web"):
    #         # Skip certain apps
    #         return True
        
    #     # Normalize app name
    #     if app.startswith("backoffice-v1-web"):
    #         app = "backoffice-v1-web"
        
    #     workflow_name = "test33"
        
    #     async with aiohttp.ClientSession() as session:
    #         # Get current workflow configuration
    #         url = f"{self.base_url}/openapi/workflows/custom/{workflow_name}/detail?projectKey={self.project_key}"
            
    #         async with session.get(url, headers=self.headers) as response:
    #             if response.status != 200:
    #                 return False
                
    #             data = await response.json()
            
    #         # Find the configuration stage
    #         stages = data.get('stages', [])
    #         updated = False
            
    #         for stage in stages:
    #             if stage.get('name') == '配置检查':
    #                 for job in stage.get('jobs', []):
    #                     if job.get('name') == '配置变更':
    #                         namespace_list = job.get('spec', {}).get('namespaceList', [])
                            
    #                         # Check if config already exists
    #                         existing = any(
    #                             ns.get('appID') == app and ns.get('clusterID') == env 
    #                             for ns in namespace_list
    #                         )
                            
    #                         if not existing:
    #                             # Add new configuration
    #                             new_config = {
    #                                 "appID": app,
    #                                 "clusterID": env,
    #                                 "env": "FAT",
    #                                 "kv": [],
    #                                 "namespace": f"web.{app}",
    #                                 "original_config": [],
    #                                 "type": "properties"
    #                             }
    #                             namespace_list.append(new_config)
    #                             updated = True
    #                             logger.info(f"Added {app} configuration for {env}")
            
    #         # Save if updated
    #         if updated:
    #             update_url = f"{self.base_url}/api/aslan/workflow/v4/{workflow_name}?projectName={self.project_key}"
                
    #             async with session.put(
    #                 update_url, 
    #                 headers=self.headers, 
    #                 data=json.dumps(data)
    #             ) as response:
    #                 if response.status >= 200 and response.status < 300:
    #                     logger.info("Updated workflow configuration")
    #                     return True
    #                 else:
    #                     logger.error("Failed to update workflow configuration")
    #                     return False
            
    #         return True