"""
Certificate manager using cert-manager and Kong
"""
import base64
import json
import asyncio
from typing import List, Dict, Optional
import aiohttp
from kubernetes import client
from kubernetes.client.rest import ApiException

from ..config.settings import kong_config, cert_manager_config
from ..utils.logger import setup_logger
from ..utils.retry import async_retry

logger = setup_logger(__name__)


class CertificateManager:
    """Manager for SSL certificate operations using cert-manager"""
    
    def __init__(self):
        self.kong_admin_url = kong_config.admin_url
        # Kubernetes API clients
        self.custom_api = client.CustomObjectsApi()
        self.v1 = client.CoreV1Api()
        # cert-manager API details
        self.cert_manager_group = "cert-manager.io"
        self.cert_manager_version = "v1"
        self.cert_namespace = cert_manager_config.namespace
        self.issuer_name = cert_manager_config.issuer_name
        self.issuer_kind = cert_manager_config.issuer_kind
    
    @async_retry(max_tries=3)
    async def create_certificate(self, env: str) -> Dict[str, str]:
        """Create SSL certificate using cert-manager"""
        domains = [
            f"*.{env}.shub.us",
            f"*.backoffice.{env}.shub.us",
            f"*.beep.{env}.shub.us",
            f"*.market.{env}.shub.us"
        ]
        
        cert_name = f"shub-us-{env}-certificate"
        secret_name = f"shub-{env}-tls"
        
        try:
            # First check if certificate already exists and is ready
            existing_secret = await self._check_existing_certificate(env, cert_name, secret_name)
            if existing_secret:
                logger.info(f"Certificate for {env} already exists and is ready, using existing")
                # Extract certificate and key from secret
                cert_data = existing_secret.data.get('tls.crt')
                key_data = existing_secret.data.get('tls.key')
                
                cert_content = base64.b64decode(cert_data).decode('utf-8')
                key_content = base64.b64decode(key_data).decode('utf-8')
                
                # Upload to Kong
                await self._upload_to_kong(env, cert_content, key_content, domains)
                
                return {
                    'cert': cert_content,
                    'key': key_content,
                    'domains': domains,
                    'secret_name': secret_name
                }
            
            # Create Certificate resource
            certificate_manifest = {
                "apiVersion": f"{self.cert_manager_group}/{self.cert_manager_version}",
                "kind": "Certificate",
                "metadata": {
                    "name": cert_name,
                    "namespace": self.cert_namespace
                },
                "spec": {
                    "secretName": secret_name,
                    "commonName": domains[0],
                    "dnsNames": domains,
                    "duration": "2160h",  # 90 days
                    "renewBefore": "720h",  # 30 days
                    "issuerRef": {
                        "name": self.issuer_name,
                        "kind": self.issuer_kind
                    }
                }
            }
            
            # Create or update the Certificate resource
            try:
                # Try to create
                await asyncio.to_thread(
                    self.custom_api.create_namespaced_custom_object,
                    group=self.cert_manager_group,
                    version=self.cert_manager_version,
                    namespace=self.cert_namespace,
                    plural="certificates",
                    body=certificate_manifest
                )
                logger.info(f"Created Certificate resource: {cert_name}")
            except ApiException as e:
                if e.status == 409:  # Already exists
                    logger.info(f"Certificate resource {cert_name} already exists, updating...")
                    await asyncio.to_thread(
                        self.custom_api.patch_namespaced_custom_object,
                        group=self.cert_manager_group,
                        version=self.cert_manager_version,
                        namespace=self.cert_namespace,
                        plural="certificates",
                        name=cert_name,
                        body=certificate_manifest
                    )
                else:
                    raise
            
            # Wait for the secret to be created
            secret = await self._wait_for_secret(secret_name, timeout=300)  # 5 minutes timeout
            
            if not secret:
                raise Exception(f"Certificate secret {secret_name} was not created within timeout")
            
            # Extract certificate and key from secret
            cert_data = secret.data.get('tls.crt')
            key_data = secret.data.get('tls.key')
            
            if not cert_data or not key_data:
                raise Exception(f"Certificate secret {secret_name} missing tls.crt or tls.key")
            
            # Decode from base64
            cert_content = base64.b64decode(cert_data).decode('utf-8')
            key_content = base64.b64decode(key_data).decode('utf-8')
            
            # Upload to Kong
            await self._upload_to_kong(env, cert_content, key_content, domains)
            
            logger.info(f"Successfully created and deployed certificate for {env}")
            return {
                'cert': cert_content,
                'key': key_content,
                'domains': domains,
                'secret_name': secret_name
            }
            
        except Exception as e:
            logger.error(f"Failed to create certificate: {e}")
            raise
    
    async def _wait_for_secret(self, secret_name: str, timeout: int = 300) -> Optional[object]:
        """Wait for secret to be created by cert-manager"""
        start_time = asyncio.get_event_loop().time()
        
        while (asyncio.get_event_loop().time() - start_time) < timeout:
            try:
                secret = await asyncio.to_thread(
                    self.v1.read_namespaced_secret,
                    name=secret_name,
                    namespace=self.cert_namespace
                )
                
                # Check if secret has certificate data
                if secret.data and 'tls.crt' in secret.data and 'tls.key' in secret.data:
                    logger.info(f"Certificate secret {secret_name} is ready")
                    return secret
                else:
                    logger.debug(f"Secret {secret_name} exists but certificate data not ready yet")
                    
            except ApiException as e:
                if e.status != 404:
                    logger.error(f"Error checking secret: {e}")
            
            # Wait before next check
            await asyncio.sleep(5)
        
        return None
    
    async def _check_existing_certificate(self, env: str, cert_name: str, secret_name: str) -> Optional[object]:
        """Check if certificate already exists and is ready"""
        try:
            # Check if Certificate resource exists
            cert = await asyncio.to_thread(
                self.custom_api.get_namespaced_custom_object,
                group=self.cert_manager_group,
                version=self.cert_manager_version,
                namespace=self.cert_namespace,
                plural="certificates",
                name=cert_name
            )
            
            # Check if certificate is ready
            conditions = cert.get('status', {}).get('conditions', [])
            is_ready = any(
                cond.get('type') == 'Ready' and cond.get('status') == 'True'
                for cond in conditions
            )
            
            if is_ready:
                # Check if secret exists with valid data
                try:
                    secret = await asyncio.to_thread(
                        self.v1.read_namespaced_secret,
                        name=secret_name,
                        namespace=self.cert_namespace
                    )
                    if secret.data and 'tls.crt' in secret.data and 'tls.key' in secret.data:
                        return secret
                except ApiException:
                    pass
                    
        except ApiException:
            # Certificate doesn't exist
            pass
        
        return None
    
    @async_retry(max_tries=3)
    async def delete_certificate(self, env: str) -> bool:
        """Delete certificate from cert-manager and Kong"""
        cert_name = f"shub-us-{env}-certificate"
        secret_name = f"shub-{env}-tls"
        
        try:
            # Delete from cert-manager
            try:
                await asyncio.to_thread(
                    self.custom_api.delete_namespaced_custom_object,
                    group=self.cert_manager_group,
                    version=self.cert_manager_version,
                    namespace=self.cert_namespace,
                    plural="certificates",
                    name=cert_name
                )
                logger.info(f"Deleted Certificate resource: {cert_name}")
            except ApiException as e:
                if e.status == 404:
                    logger.info(f"Certificate resource {cert_name} not found, skipping")
                else:
                    logger.warning(f"Failed to delete Certificate resource: {e}")
            
            # Delete secret if it exists
            try:
                await asyncio.to_thread(
                    self.v1.delete_namespaced_secret,
                    name=secret_name,
                    namespace=self.cert_namespace
                )
                logger.info(f"Deleted certificate secret: {secret_name}")
            except ApiException as e:
                if e.status == 404:
                    logger.info(f"Certificate secret {secret_name} not found, skipping")
                else:
                    logger.warning(f"Failed to delete certificate secret: {e}")
            
            # Delete from Kong
            await self._delete_from_kong(env)
            
            logger.info(f"Successfully deleted certificate for {env}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete certificate: {e}")
            return False
    
    async def _upload_to_kong(self, env: str, cert: str, key: str, snis: List[str]) -> None:
        """Upload certificate to Kong"""
        data = {
            'cert': cert,
            'key': key,
            'snis': snis
        }
        
        async with aiohttp.ClientSession() as session:
            headers = {"Content-Type": "application/json"}
            
            async with session.post(
                f"{self.kong_admin_url}/certificates",
                headers=headers,
                data=json.dumps(data)
            ) as response:
                if response.status == 201:
                    logger.info(f"Certificate uploaded to Kong for {env}")
                elif response.status == 409:
                    logger.info(f"Certificate already exists in Kong for {env}, skipping")
                elif response.status == 400:
                    error_text = await response.text()
                    if 'already associated' in error_text:
                        logger.info(f"Certificate SNIs already associated in Kong for {env}, skipping")
                    else:
                        raise Exception(f"Kong upload failed: {error_text}")
                else:
                    error_text = await response.text()
                    raise Exception(f"Kong upload failed: {error_text}")
    
    async def _delete_from_kong(self, env: str) -> None:
        """Delete certificate from Kong"""
        async with aiohttp.ClientSession() as session:
            # Get all certificates
            async with session.get(f"{self.kong_admin_url}/certificates") as response:
                response.raise_for_status()
                data = await response.json()
                
                # Find and delete certificate for this environment
                for cert in data.get('data', []):
                    if any(f"*.{env}.shub.us" in sni for sni in cert.get('snis', [])):
                        async with session.delete(
                            f"{self.kong_admin_url}/certificates/{cert['id']}"
                        ) as delete_response:
                            delete_response.raise_for_status()
                            logger.info(f"Deleted certificate {cert['id']} from Kong")