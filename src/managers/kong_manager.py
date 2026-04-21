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
                    service_name = service.get('name') or ''
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

    @async_retry(max_tries=3, exceptions=(aiohttp.ClientError,))
    async def ensure_subenv_deployment_routes(self, deployment_name: str, env: str) -> bool:
        """Create deployment-specific routes for monitored sub-environments."""
        if deployment_name not in ("backoffice-v1-web-app", "backoffice-v1-web-api"):
            return True

        async with aiohttp.ClientSession() as session:
            headers = {"Content-Type": "application/json"}

            if deployment_name == "backoffice-v1-web-app":
                service_name = f"{deployment_name}-{env}"
                await self._ensure_service(
                    session,
                    service_name=service_name,
                    host=f"{deployment_name}.{env}",
                    port=80,
                    protocol="http",
                    headers=headers,
                )
                await self._ensure_pre_function_plugin(session, service_name, headers)
                await self._ensure_route(
                    session,
                    service_name=service_name,
                    route_payload={
                        "hosts": [f"*.backoffice.{env}.shub.us"],
                        "preserve_host": True,
                        "protocols": ["http", "https"],
                        "https_redirect_status_code": 301,
                        "path_handling": "v1",
                        "strip_path": True,
                    },
                    headers=headers,
                )

                assets_service = f"bo-v1-assets-{env}"
                await self._ensure_service(
                    session,
                    service_name=assets_service,
                    host=f"bo-v1-assets.{env}",
                    port=80,
                    protocol="http",
                    headers=headers,
                )
                await self._ensure_route(
                    session,
                    service_name=assets_service,
                    route_payload={
                        "hosts": [f"*.backoffice.{env}.shub.us"],
                        "protocols": ["http", "https"],
                        "https_redirect_status_code": 301,
                        "preserve_host": True,
                        "strip_path": False,
                        "path_handling": "v1",
                        "paths": ["/img", "/scripts", "/assets", "/css", "/ico", "/files"],
                    },
                    headers=headers,
                )
                return True

            # backoffice-v1-web-api
            service_name = f"{deployment_name}-{env}"
            await self._ensure_service(
                session,
                service_name=service_name,
                host=f"{deployment_name}.{env}",
                port=80,
                protocol="http",
                headers=headers,
            )
            await self._ensure_route(
                session,
                service_name=service_name,
                route_payload={
                    "hosts": [f"backoffice-api.{env}.shub.us"],
                    "preserve_host": False,
                    "protocols": ["http", "https"],
                    "https_redirect_status_code": 301,
                    "path_handling": "v1",
                    "strip_path": True,
                },
                headers=headers,
            )
            return True

    @async_retry(max_tries=3, exceptions=(aiohttp.ClientError,))
    async def remove_subenv_deployment_routes(self, deployment_name: str, env: str) -> bool:
        """Delete deployment-specific routes for monitored sub-environments."""
        # Keep behavior aligned with demo script: only web-app deletion path.
        if deployment_name != "backoffice-v1-web-app":
            return True

        async with aiohttp.ClientSession() as session:
            await self._delete_service_routes_by_env(session, f"backoffice-v1-web-app-{env}", env)
            await self._delete_service_if_exists(session, f"backoffice-v1-web-app-{env}")
            await self._delete_service_routes_by_env(session, f"bo-v1-assets-{env}", env)
            await self._delete_service_if_exists(session, f"bo-v1-assets-{env}")
            return True

    async def _ensure_service(
        self,
        session: aiohttp.ClientSession,
        service_name: str,
        host: str,
        port: int,
        protocol: str,
        headers: Dict[str, str],
    ):
        async with session.get(f"{self.admin_url}/services/{service_name}") as resp:
            if resp.status == 200:
                return

        payload = {"name": service_name, "port": port, "protocol": protocol, "host": host}
        async with session.post(
            f"{self.admin_url}/services",
            headers=headers,
            data=json.dumps(payload),
        ) as resp:
            if resp.status not in (201, 409):
                error_text = await resp.text()
                raise RuntimeError(f"Failed to create service {service_name}: {error_text}")

    async def _ensure_pre_function_plugin(self, session: aiohttp.ClientSession, service_name: str, headers: Dict[str, str]):
        async with session.get(f"{self.admin_url}/services/{service_name}/plugins") as resp:
            if resp.status == 200:
                data = await resp.json()
                for plugin in data.get("data", []):
                    if plugin.get("name") == "pre-function":
                        return

        payload = {
            "protocols": ["grpc", "grpcs", "http", "https"],
            "enabled": True,
            "config": {
                "certificate": [],
                "rewrite": [],
                "access": [
                    "local random = math.random local function uuid() local template = \"xx-x4xxxxyxxyyyyxxyx4xxxxyxxyyyyxxy-yxxxxxyxyyxyxyxx-xx\" local ans = string.gsub(template, \"[xy]\", function(c) local v = (c == \"x\") and random(0, 0xf) or random(8, 0xb) return string.format(\"%x\", v) end) return ans end kong.service.request.add_header(\"traceparent\", uuid())",
                    "function split(s, delimiter) res = {} for match in (s .. delimiter):gmatch(\"([^.]+)\" .. delimiter) do table.insert(res, match) end if #res == 5 then return res[3] else return res[2] end end local host = kong.request.get_host() local env = split(host, \".\") kong.service.request.add_header(\"x-env\", env)",
                ],
                "header_filter": [],
                "body_filter": [],
                "log": [],
            },
            "name": "pre-function",
        }
        async with session.post(
            f"{self.admin_url}/services/{service_name}/plugins",
            headers=headers,
            data=json.dumps(payload),
        ) as resp:
            if resp.status not in (201, 409):
                error_text = await resp.text()
                raise RuntimeError(f"Failed to create pre-function plugin for {service_name}: {error_text}")

    async def _ensure_route(
        self,
        session: aiohttp.ClientSession,
        service_name: str,
        route_payload: Dict[str, Any],
        headers: Dict[str, str],
    ):
        desired_hosts = set(route_payload.get("hosts") or [])
        desired_paths = set(route_payload.get("paths") or [])

        async with session.get(f"{self.admin_url}/services/{service_name}/routes") as resp:
            if resp.status == 200:
                data = await resp.json()
                for route in data.get("data", []):
                    route_hosts = set(route.get("hosts") or [])
                    route_paths = set(route.get("paths") or [])
                    if route_hosts == desired_hosts and route_paths == desired_paths:
                        return

        async with session.post(
            f"{self.admin_url}/services/{service_name}/routes",
            headers=headers,
            data=json.dumps(route_payload),
        ) as resp:
            if resp.status not in (201, 409):
                error_text = await resp.text()
                raise RuntimeError(f"Failed to create route for {service_name}: {error_text}")

    async def _delete_service_routes_by_env(self, session: aiohttp.ClientSession, service_name: str, env: str):
        async with session.get(f"{self.admin_url}/services/{service_name}/routes") as resp:
            if resp.status != 200:
                return
            data = await resp.json()
            routes = data.get("data", [])

        for route in routes:
            hosts = route.get("hosts") or []
            if not any(env in host for host in hosts):
                continue
            route_id = route.get("id")
            if not route_id:
                continue
            async with session.delete(f"{self.admin_url}/routes/{route_id}") as delete_resp:
                if delete_resp.status not in (204, 404):
                    error_text = await delete_resp.text()
                    raise RuntimeError(f"Failed to delete route {route_id}: {error_text}")

    async def _delete_service_if_exists(self, session: aiohttp.ClientSession, service_name: str):
        async with session.delete(f"{self.admin_url}/services/{service_name}") as resp:
            if resp.status not in (204, 404):
                error_text = await resp.text()
                raise RuntimeError(f"Failed to delete service {service_name}: {error_text}")
