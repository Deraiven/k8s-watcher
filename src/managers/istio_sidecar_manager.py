"""
Istio Sidecar manager for limiting cross-namespace config scope.
"""
import asyncio
from typing import Dict, List

from kubernetes import client
from kubernetes.client.rest import ApiException

from ..config.settings import app_config
from ..utils.logger import setup_logger
from ..utils.retry import async_retry

logger = setup_logger(__name__)


class IstioSidecarManager:
    """Manage per-sub-environment Istio Sidecar scope resources."""

    def __init__(self):
        self.custom_api = client.CustomObjectsApi()
        self.group = "networking.istio.io"
        self.version = "v1beta1"
        self.plural = "sidecars"
        self.sidecar_name = "vs-scope"
        self.reference_env = app_config.reference_env

    def _build_sidecar_manifest(self, env: str) -> Dict:
        return {
            "apiVersion": f"{self.group}/{self.version}",
            "kind": "Sidecar",
            "metadata": {
                "name": self.sidecar_name,
                "namespace": env,
            },
            "spec": {
                "egress": [
                    {
                        "hosts": [
                            "./*",
                            f"{self.reference_env}/*",
                        ]
                    }
                ]
            },
        }

    @async_retry(max_tries=3)
    async def ensure_sidecar_scope(self, env: str) -> Dict:
        """Create or patch the Sidecar that limits cross-namespace config visibility."""
        manifest = self._build_sidecar_manifest(env)
        try:
            result = await asyncio.to_thread(
                self.custom_api.create_namespaced_custom_object,
                group=self.group,
                version=self.version,
                namespace=env,
                plural=self.plural,
                body=manifest,
            )
            logger.info("Created Istio Sidecar scope: namespace=%s name=%s", env, self.sidecar_name)
            return result
        except ApiException as e:
            if e.status != 409:
                logger.error("Failed to create Istio Sidecar scope for %s: %s", env, e)
                raise

        result = await asyncio.to_thread(
            self.custom_api.patch_namespaced_custom_object,
            group=self.group,
            version=self.version,
            namespace=env,
            plural=self.plural,
            name=self.sidecar_name,
            body=manifest,
        )
        logger.info("Patched Istio Sidecar scope: namespace=%s name=%s", env, self.sidecar_name)
        return result

    @async_retry(max_tries=3)
    async def delete_sidecar_scope(self, env: str) -> bool:
        """Delete the per-namespace Sidecar scope resource."""
        try:
            await asyncio.to_thread(
                self.custom_api.delete_namespaced_custom_object,
                group=self.group,
                version=self.version,
                namespace=env,
                plural=self.plural,
                name=self.sidecar_name,
            )
            logger.info("Deleted Istio Sidecar scope: namespace=%s name=%s", env, self.sidecar_name)
            return True
        except ApiException as e:
            if e.status == 404:
                logger.info("Istio Sidecar scope not found, skipping: namespace=%s name=%s", env, self.sidecar_name)
                return True
            logger.error("Failed to delete Istio Sidecar scope for %s: %s", env, e)
            raise

    async def reconcile_sidecar_scopes(self, namespaces: List[str]) -> None:
        """Ensure Sidecar scope exists for all currently tracked namespaces."""
        if not namespaces:
            return
        logger.info("Reconciling Istio Sidecar scopes for %s namespaces", len(namespaces))
        results = await asyncio.gather(
            *(self.ensure_sidecar_scope(namespace) for namespace in namespaces),
            return_exceptions=True,
        )
        success_count = 0
        for namespace, result in zip(namespaces, results):
            if isinstance(result, Exception):
                logger.error("Failed to reconcile Istio Sidecar scope for %s: %s", namespace, result)
            else:
                success_count += 1
        logger.info("Istio Sidecar scope reconciliation completed: %s/%s successful", success_count, len(namespaces))
