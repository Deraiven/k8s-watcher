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
        """Update backend workflow (test33)"""
        workflow_name = "test33"
        
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
            if param.get('name') == '环境':
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
        if action == 'delete':
            stages = data.get('stages', [])
            for stage in stages:
                if stage.get('name') == '配置检查':
                    for job in stage.get('jobs', []):
                        if job.get('name') == '配置变更':
                            namespace_list = job.get('spec', {}).get('namespaceList', [])
                            job['spec']['namespaceList'] = [
                                ns for ns in namespace_list 
                                if ns.get('clusterID') != env
                            ]
        
        # Save updated workflow
        if params_updated or action == 'delete':
            update_data = {
                "name": workflow_name,
                "display_name": data.get('display_name', 'fat-base-workflow'),
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
    
    @async_retry(max_tries=3, exceptions=(aiohttp.ClientError,))
    async def add_git_trigger(self, service_name: str, repo_owner: str, branch: str, events: List[str], workflow_name: str = "test33") -> bool:
        """Add a Git trigger to a Zadig workflow for a specific service and branch."""
        async with aiohttp.ClientSession() as session:
            try:
                # Get current workflow configuration
                url = f"{self.base_url}/openapi/workflows/custom/{workflow_name}/detail?projectKey={self.project_key}"

                async with session.get(url, headers=self.headers) as response:
                    if response.status != 200:
                        logger.error(f"Failed to get workflow {workflow_name}: {response.status}")
                        return False
                    data = await response.json()

                # Ensure 'triggers' list exists
                if 'triggers' not in data:
                    data['triggers'] = []

                # Dynamically determine repo_name from workflow detail
                repo_name = zadig_config.repo_name_template.format(service_name=service_name)

                # Create new trigger object
                new_trigger = {
                    "type": "git",
                    "repo_owner": repo_owner,
                    "repo_name": repo_name,
                    "branch": branch,
                    "events": events,
                    "service_name": service_name,
                    "auto_deploy": True,
                }

                # Check if trigger already exists to avoid duplicates
                trigger_exists = any(
                    t.get("repo_name") == new_trigger["repo_name"] and
                    t.get("branch") == new_trigger["branch"] and
                    t.get("service_name") == new_trigger["service_name"]
                    for t in data["triggers"]
                )

                if not trigger_exists:
                    data["triggers"].append(new_trigger)
                    logger.info(f"Added Git trigger for service {service_name} on repo {repo_owner}/{repo_name}, branch {branch}")
                else:
                    logger.info(f"Git trigger for service {service_name} on repo {repo_owner}/{repo_name}, branch {branch} already exists.")
                    return True # Consider it a success if it already exists

                # Save updated workflow
                update_url = f"{self.base_url}/api/aslan/workflow/v4/{workflow_name}?projectName={self.project_key}"

                async with session.put(update_url, headers=self.headers, data=json.dumps(data)) as response:
                    if response.status >= 200 and response.status < 300:
                        logger.info(f"Updated workflow {workflow_name} with new Git trigger.")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to update workflow with Git trigger: {error_text}")
                        return False

            except Exception as e:
                logger.error(f"Error adding Git trigger to workflow: {e}")
                raise