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
    async def add_git_trigger(self, service_name: str, repo_owner: str, branch: str, events: List[str], workflow_name: str = "test33", env: str = None) -> bool:
        """Add a Git trigger to a Zadig workflow for a specific service and branch."""
        async with aiohttp.ClientSession() as session:
            try:
                # Step 1: Check existing webhooks using webhook API
                webhook_url = f"{self.base_url}/api/aslan/workflow/v4/webhook?projectName={self.project_key}&workflowName={workflow_name}"
                
                async with session.get(webhook_url, headers=self.headers) as response:
                    if response.status == 200:
                        existing_webhooks = await response.json()
                        
                        # Check if webhook already exists for this service and branch
                        repo_name = zadig_config.repo_name_template.format(service_name=service_name)
                        
                        for webhook in existing_webhooks:
                            # Check in main_repo structure (webhook API response format)
                            if 'main_repo' in webhook:
                                main_repo = webhook['main_repo']
                                if (main_repo.get('repo_owner') == repo_owner and 
                                    main_repo.get('repo_name') == repo_name and 
                                    main_repo.get('branch') == branch):
                                    logger.info(f"Git trigger for service {service_name} on repo {repo_owner}/{repo_name}, branch {branch} already exists in webhooks.")
                                    return True
                    else:
                        logger.warning(f"Could not fetch webhooks, continuing with webhook creation: {response.status}")

                # Step 2: Get workflow detail for creating webhook
                workflow_detail_url = f"{self.base_url}/openapi/workflows/custom/{workflow_name}/detail?projectKey={self.project_key}"
                
                async with session.get(workflow_detail_url, headers=self.headers) as response:
                    if response.status != 200:
                        logger.error(f"Failed to get workflow detail: {response.status}")
                        return False
                    
                    workflow_detail = await response.json()
                
                # Step 3: Create webhook using the correct API format
                repo_name = zadig_config.repo_name_template.format(service_name=service_name)
                webhook_name = f"{service_name}-{env if env else branch}".replace("/", "-")
                
                # Prepare webhook data based on the captured request
                webhook_data = {
                    "check_patch_set_change": False,
                    "name": webhook_name,
                    "auto_cancel": True,
                    "is_manual": False,
                    "enabled": True,
                    "description": "",
                    "repos": [],
                    "main_repo": {
                        "source": "github",
                        "repo_owner": repo_owner,
                        "repo_namespace": repo_owner,
                        "repo_name": repo_name,
                        "branch": branch,
                        "tag": "",
                        "label": "",
                        "committer": "",
                        "match_folders": ["/", "!.md"],
                        "codehost_id": 6,
                        "is_regular": False,
                        "events": events,
                        "remote_name": "origin",
                        "enable_commit": False,
                        "hidden": False,
                        "is_primary": False,
                        "oauth_token": "",
                        "address": "",
                        "filter_regexp": ".*",
                        "source_from": "",
                        "param_name": "",
                        "job_name": "",
                        "service_name": "",
                        "service_module": "",
                        "repo_index": 0,
                        "submission_id": "",
                        "disable_ssl": False,
                        "key": f"{repo_owner}/{repo_name}"
                    },
                    "workflow_arg": workflow_detail
                }
                
                # Ensure required fields are set in workflow_arg
                if 'project' not in webhook_data['workflow_arg']:
                    webhook_data['workflow_arg']['project'] = self.project_key
                
                # Also ensure project_key is converted to project
                if 'project_key' in webhook_data['workflow_arg'] and 'project' not in webhook_data['workflow_arg']:
                    webhook_data['workflow_arg']['project'] = webhook_data['workflow_arg']['project_key']
                
                # Convert workflow_key to name and workflow_name to display_name
                if 'workflow_key' in webhook_data['workflow_arg']:
                    webhook_data['workflow_arg']['name'] = webhook_data['workflow_arg']['workflow_key']
                if 'workflow_name' in webhook_data['workflow_arg']:
                    webhook_data['workflow_arg']['display_name'] = webhook_data['workflow_arg']['workflow_name']
                
                # Clean up Apollo configuration to avoid "not allowed to be changed" error
                if 'stages' in webhook_data['workflow_arg']:
                    for stage in webhook_data['workflow_arg']['stages']:
                        if stage.get('name') == '配置检查':
                            for job in stage.get('jobs', []):
                                if job.get('type') == 'apollo' and 'spec' in job:
                                    # Clear namespace list to avoid Apollo env change errors
                                    job['spec']['namespaceList'] = []
                
                # Update workflow_arg value parameter if env is provided
                if env and 'params' in webhook_data['workflow_arg']:
                    for param in webhook_data['workflow_arg']['params']:
                        if param.get('name') == '环境':
                            param['value'] = env
                            break
                
                # Create webhook using POST method to the correct endpoint
                create_webhook_url = f"{self.base_url}/api/aslan/workflow/v4/webhook/{workflow_name}?projectName={self.project_key}"
                
                async with session.post(create_webhook_url, headers=self.headers, json=webhook_data) as response:
                    if response.status >= 200 and response.status < 300:
                        logger.info(f"Successfully created Git trigger webhook '{webhook_name}' for service {service_name} on branch {branch}")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to create webhook: {response.status} - {error_text}")
                        
                        # If webhook creation fails, try the old approach as fallback
                        logger.info("Falling back to workflow update approach...")
                        return await self._add_trigger_to_workflow(session, workflow_name, service_name, repo_owner, branch, events)

            except Exception as e:
                logger.error(f"Error adding Git trigger to workflow: {e}")
                raise
    
    async def _add_trigger_to_workflow(self, session, workflow_name: str, service_name: str, repo_owner: str, branch: str, events: List[str]) -> bool:
        """Fallback method to add trigger by updating workflow configuration"""
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

            # Dynamically determine repo_name from template
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

            # Check if trigger already exists in workflow configuration
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
                logger.info(f"Git trigger for service {service_name} on repo {repo_owner}/{repo_name}, branch {branch} already exists in workflow config.")
                return True

            # Save updated workflow
            update_data = {
                "name": workflow_name,
                "display_name": data.get('display_name', data.get('workflow_name', 'fat-base-workflow')),
                "concurrency_limit": data.get('concurrency_limit', 10),
                "project": self.project_key,
                "params": data.get('params', []),
                "stages": data.get('stages', []),
                "triggers": data.get('triggers', [])
            }
            
            update_url = f"{self.base_url}/api/aslan/workflow/v4/{workflow_name}?projectName={self.project_key}"

            async with session.put(update_url, headers=self.headers, data=json.dumps(update_data)) as response:
                if response.status >= 200 and response.status < 300:
                    logger.info(f"Updated workflow {workflow_name} with new Git trigger.")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to update workflow with Git trigger: {error_text}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error in fallback trigger method: {e}")
            return False