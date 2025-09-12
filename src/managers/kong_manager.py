"""
Kong API Gateway manager
"""
import json
from typing import List, Dict, Any, Optional
import aiohttp

from ..config.settings import kong_config, app_config
from ..utils.logger import setup_logger
from ..utils.retry import async_retry

logger = setup_logger(__name__)


class KongManager:
    """Manager for Kong API Gateway operations"""
    
    def __init__(self):
        self.admin_url = kong_config.admin_url
        self.reference_env = app_config.reference_env
    
    @async_retry(max_tries=3, exceptions=(aiohttp.ClientError,))
    async def create_routes(self, env: str) -> List[str]:
        """Create routes for new environment by copying from reference environment"""
        created_routes = []
        
        async with aiohttp.ClientSession() as session:
            try:
                # Get all services
                async with session.get(f"{self.admin_url}/services?size=1000") as response:
                    response.raise_for_status()
                    services_data = await response.json()
                    services = services_data.get('data', [])
                
                # Process services that match reference environment
                for service in services:
                    service_name = service.get('name')
                    
                    if not service_name or self.reference_env not in service_name:
                        continue
                    
                    # Skip specific services that shouldn't be copied
                    skip_services = [
                        f"backoffice-v1-web-app-{self.reference_env}",
                        f"bo-v1-assets-{self.reference_env}",
                        f"backoffice-v1-web-api-{self.reference_env}"
                    ]
                    if service_name in skip_services:
                        continue
                    
                    # Get routes for this service
                    async with session.get(f"{self.admin_url}/services/{service_name}/routes") as route_response:
                        route_response.raise_for_status()
                        routes_data = await route_response.json()
                        routes = routes_data.get('data', [])
                    
                    # Check if routes for this env already exist
                    existing_hosts = []
                    for route in routes:
                        existing_hosts.extend(route.get('hosts', []))
                    
                    if any(f"{env}." in host for host in existing_hosts):
                        logger.info(f"Routes already exist for {service_name} in {env}")
                        continue
                    
                    # Create new routes
                    for route in routes:
                        ref_hosts = [h for h in route.get('hosts', []) if self.reference_env in h]
                        if not ref_hosts:
                            continue
                        
                        # Create route data for new environment
                        route_data = {
                            'hosts': [h.replace(self.reference_env, env) for h in route.get('hosts', [])],
                            'protocols': route.get('protocols', ['http', 'https']),
                            'paths': route.get('paths'),
                            'https_redirect_status_code': route.get('https_redirect_status_code', 426),
                            'path_handling': route.get('path_handling', 'v0'),
                            'preserve_host': route.get('preserve_host', False)
                        }
                        
                        # Create the route
                        headers = {"Content-Type": "application/json"}
                        async with session.post(
                            f"{self.admin_url}/services/{service_name}/routes",
                            headers=headers,
                            data=json.dumps(route_data)
                        ) as create_response:
                            if create_response.status == 201:
                                route_id = f"{service_name}-{env}"
                                created_routes.append(route_id)
                                logger.info(f"Created route for service {service_name}")
                            elif create_response.status == 409:
                                logger.info(f"Route already exists for service {service_name}, skipping")
                            elif create_response.status == 400:
                                error_text = await create_response.text()
                                if 'already exists' in error_text or 'duplicate' in error_text.lower():
                                    logger.info(f"Route configuration already exists for {service_name}, skipping")
                                else:
                                    logger.error(f"Failed to create route: {error_text}")
                            else:
                                error_text = await create_response.text()
                                logger.error(f"Failed to create route: {error_text}")
                
                return created_routes
                
            except Exception as e:
                logger.error(f"Failed to create routes: {e}")
                raise
    
    @async_retry(max_tries=3, exceptions=(aiohttp.ClientError,))
    async def delete_routes(self, env: str) -> List[str]:
        """Delete routes and services for an environment"""
        deleted_items = []
        
        async with aiohttp.ClientSession() as session:
            try:
                # Delete routes
                async with session.get(f"{self.admin_url}/routes?size=1000") as response:
                    response.raise_for_status()
                    routes_data = await response.json()
                    routes = routes_data.get('data', [])
                
                # Delete routes that match this environment
                for route in routes:
                    hosts = route.get('hosts', [])
                    if any(f"{env}." in host for host in hosts):
                        route_id = route['id']
                        async with session.delete(f"{self.admin_url}/routes/{route_id}") as delete_response:
                            if delete_response.status in (204, 404):
                                deleted_items.append(f"route-{route_id}")
                                logger.info(f"Deleted route {route_id}")
                
                # Delete services
                async with session.get(f"{self.admin_url}/services?size=1000") as response:
                    response.raise_for_status()
                    services_data = await response.json()
                    services = services_data.get('data', [])
                
                # Delete services that end with this environment
                for service in services:
                    service_name = service.get('name', '')
                    if service_name.endswith(env):
                        service_id = service['id']
                        
                        # First delete associated plugins
                        async with session.get(f"{self.admin_url}/services/{service_id}/plugins") as plugin_response:
                            if plugin_response.status == 200:
                                plugins_data = await plugin_response.json()
                                for plugin in plugins_data.get('data', []):
                                    async with session.delete(f"{self.admin_url}/plugins/{plugin['id']}") as delete_plugin:
                                        if delete_plugin.status in (204, 404):
                                            logger.info(f"Deleted plugin {plugin['id']} for service {service_name}")
                        
                        # Then delete the service
                        async with session.delete(f"{self.admin_url}/services/{service_id}") as delete_response:
                            if delete_response.status in (204, 404):
                                deleted_items.append(f"service-{service_name}")
                                logger.info(f"Deleted service {service_name}")
                
                return deleted_items
                
            except Exception as e:
                logger.error(f"Failed to delete routes: {e}")
                raise
    
    async def get_service_info(self, service_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific service"""
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(f"{self.admin_url}/services/{service_name}") as response:
                    if response.status == 200:
                        return await response.json()
                    return None
            except Exception as e:
                logger.error(f"Failed to get service info: {e}")
                return None