"""Apollo 2.4 configuration management through the Portal OpenAPI."""
import asyncio
import base64
import hashlib
import hmac
import re
from email.utils import formatdate
from typing import Dict, List
from urllib.parse import quote, urlencode

import aiohttp
from yarl import URL

from ..config.settings import apollo_config, app_config
from ..utils.logger import setup_logger

logger = setup_logger(__name__)


class ApolloAPIError(RuntimeError):
    """Sanitized API failure; response bodies may contain configuration secrets."""

    def __init__(self, method, path, status):
        self.status = status
        super().__init__(f"Apollo OpenAPI {method} {path} failed: HTTP {status}")


class ApolloManager:
    """Create configuration only for applications present in a sub-environment."""

    def __init__(self):
        self.base_url = apollo_config.url.rstrip("/")
        self.token = (apollo_config.token or "").strip()
        self.apollo_env = apollo_config.env
        self.operator = apollo_config.operator
        self.timeout = apollo_config.timeout_seconds
        self.admin_url = apollo_config.admin_url.rstrip("/")
        self.hmac_username = apollo_config.hmac_username
        self.hmac_secret = apollo_config.hmac_secret
        if not self.token:
            raise ValueError("APOLLO_API_TOKEN is required when ENABLE_APOLLO_CONFIG=true")
        if any(ord(char) < 32 or ord(char) == 127 for char in self.token):
            raise ValueError("APOLLO_API_TOKEN contains an embedded control character; check the injected Secret")
        if not self.operator:
            raise ValueError("APOLLO_OPERATOR must be an existing Apollo user")
        self.reference_env = app_config.reference_env
        self._lock = asyncio.Lock()
        self._deployment_aliases = {
            "backoffice-v1-web-app": "backoffice-v2-webapp",
            "beep-v1-web": "beep-v1-webapp",
            "online-purchase-svc-cronjob": "online-purchase-svc",
        }
        self._skip_apps = {"bo-v1-assets", "inventory-cronjob"}

    def _cluster_path(self, app_id, cluster):
        segments = (self.apollo_env, app_id, cluster)
        env, app, name = (quote(value, safe="") for value in segments)
        return f"/openapi/v1/envs/{env}/apps/{app}/clusters/{name}"

    async def _request(self, session, method, path, *, missing_ok=False, **kwargs):
        # Only GETs are automatically retried: a timed-out write may have succeeded.
        for attempt in range(3):
            try:
                async with session.request(
                    method, self.base_url + path, allow_redirects=False, **kwargs
                ) as response:
                    if response.status == 404 and missing_ok:
                        return None
                    if response.status == 429 or response.status >= 500:
                        if method == "GET" and attempt < 2:
                            await asyncio.sleep(2 ** attempt)
                            continue
                    if not 200 <= response.status < 300:
                        raise ApolloAPIError(method, path, response.status)
                    if response.status == 204:
                        return None
                    return await response.json()
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if method == "GET" and attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    continue
                raise RuntimeError(f"Apollo OpenAPI {method} {path} transport failure") from None

    def _session(self):
        # Apollo expects the raw token, not a Bearer-prefixed Authorization value.
        return aiohttp.ClientSession(
            headers={"Authorization": self.token},
            timeout=aiohttp.ClientTimeout(total=self.timeout),
        )

    async def delete_cluster_config(self, env: str) -> bool:
        if (not re.fullmatch(r"test[0-9]+", env) or env == self.reference_env
                or env in app_config.excluded_namespaces):
            raise ValueError(f"Refusing to delete protected or invalid Apollo cluster {env}")
        self._validate_admin_config()
        async with self._lock, aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout)
        ) as session:
            # Finish enumeration before deleting; never use token-scoped Portal apps.
            apps = set()
            page = 0
            while True:
                batch = await self._admin_request(session, "GET", f"/apps?page={page}&size=100")
                if not isinstance(batch, list) or any(
                    not isinstance(app, dict) or not isinstance(app.get("appId"), str)
                    or not app["appId"] for app in batch
                ):
                    raise RuntimeError("Invalid Apollo Admin app listing")
                if not batch:
                    break
                identifiers = {app["appId"] for app in batch}
                if identifiers & apps:
                    raise RuntimeError("Apollo Admin pagination repeated apps; cleanup aborted")
                apps.update(identifiers)
                page += 1
            deleted = 0
            for app_id in sorted(apps):
                path = f"/apps/{quote(app_id, safe='')}/clusters/{quote(env, safe='')}"
                cluster = await self._admin_request(session, "GET", path, missing_ok=True)
                if cluster is None:
                    continue
                if not isinstance(cluster, dict) or cluster.get("name") != env or cluster.get("appId") != app_id:
                    raise RuntimeError("Apollo Admin returned a mismatched cluster; cleanup aborted")
                await self._admin_request(session, "DELETE", path + "?" + urlencode({"operator": self.operator}),
                                          missing_ok=True)
                deleted += 1
                logger.info("Deleted Apollo cluster app=%s cluster=%s", app_id, env)
        logger.info("Successfully deleted Apollo configuration for %s, clusters_deleted=%s", env, deleted)
        return True

    def _admin_headers(self, method, path):
        if any(char in self.hmac_username for char in ('"', '\\', '\r', '\n')):
            raise ValueError("Invalid KONG_HMAC_USERNAME")
        date = formatdate(usegmt=True)
        signing_string = f"date: {date}\nrequest-line: {method} {path} HTTP/1.1"
        signature = base64.b64encode(hmac.new(
            self.hmac_secret.encode(), signing_string.encode(), hashlib.sha256
        ).digest()).decode()
        return {"Date": date, "Authorization": (
            f'hmac username="{self.hmac_username}", algorithm="hmac-sha256", '
            f'headers="date request-line", signature="{signature}"'
        )}

    def _validate_admin_config(self):
        if not self.hmac_username or not self.hmac_secret:
            raise ValueError("KONG_HMAC_USERNAME and KONG_HMAC_SECRET are required for Apollo Admin operations")
        base = URL(self.admin_url)
        if (base.scheme != "https" or not base.host or base.user is not None
                or base.path != "/" or base.query_string or base.fragment):
            raise ValueError("APOLLO_ADMIN_URL must be an HTTPS origin without a path or credentials")

    async def _admin_request(self, session, method, path, *, missing_ok=False, **kwargs):
        self._validate_admin_config()
        # Preserve the exact escaped request target used in the HMAC signature.
        url = URL(self.admin_url + path, encoded=True)
        try:
            async with session.request(method, url, headers=self._admin_headers(method, path),
                                       allow_redirects=False, **kwargs) as response:
                if response.status == 404 and missing_ok:
                    return None
                if not 200 <= response.status < 300:
                    raise ApolloAPIError(method, path, response.status)
                # Admin DELETE returns an empty 200 response, not necessarily 204.
                if method == "DELETE":
                    return None
                return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
            raise RuntimeError(f"Apollo Admin {method} transport or response failure") from None

    async def _ensure_web_namespace(self, app_id, env):
        """Attach the existing web namespace, without creating a global AppNamespace."""
        self._validate_admin_config()
        name = f"web.{app_id}"
        path = f"/apps/{quote(app_id, safe='')}/clusters/{quote(env, safe='')}/namespaces"
        detail = path + "/" + quote(name, safe="")
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
            existing = await self._admin_request(session, "GET", detail, missing_ok=True)
            if existing is None:
                try:
                    await self._admin_request(session, "POST", path, json={
                        "appId": app_id, "clusterName": env, "namespaceName": name,
                        "dataChangeCreatedBy": self.operator,
                        "dataChangeLastModifiedBy": self.operator,
                    })
                except ApolloAPIError as error:
                    # Another watcher may have attached it between GET and POST.
                    if error.status not in (400, 409):
                        raise
                    existing = await self._admin_request(session, "GET", detail, missing_ok=True)
                    if existing is None:
                        raise
                else:
                    existing = await self._admin_request(session, "GET", detail)
            if (not isinstance(existing, dict) or existing.get("appId") != app_id
                    or existing.get("clusterName") != env or existing.get("namespaceName") != name):
                raise RuntimeError("Apollo Admin returned a mismatched namespace")
        logger.info("Apollo namespace ensured app=%s cluster=%s namespace=%s", app_id, env, name)

    async def get_cluster_apps(self, env: str) -> List[Dict[str, str]]:
        results = []
        async with self._session() as session:
            apps = await self._request(session, "GET", "/openapi/v1/apps/authorized")
            for app in apps:
                path = self._cluster_path(app["appId"], env)
                if await self._request(session, "GET", path, missing_ok=True) is None:
                    continue
                namespaces = await self._request(session, "GET", path + "/namespaces",
                                                 params={"fillItemDetail": "false"})
                results.extend({"AppId": app["appId"], "NamespaceName": ns["namespaceName"]}
                               for ns in namespaces)
        return results

    def _normalize_app_id(self, deployment_name: str) -> str:
        app_id = self._deployment_aliases.get(deployment_name, deployment_name)
        if app_id.startswith("backoffice-v1-web"):
            return "backoffice-v1-web"
        return app_id

    def _resolve_target_app_ids(self, deployment_name: str) -> List[str]:
        if deployment_name == "backoffice-v1-web-app":
            return ["backoffice-v2-webapp", "backoffice-v1-web"]
        if deployment_name == "backoffice-v1-web" or deployment_name.startswith("backoffice-v1-web-"):
            return ["backoffice-v1-web"]
        if deployment_name == "beep-v1-web":
            return ["beep-v1-web", "beep-v1-webapp"]
        return [self._normalize_app_id(deployment_name)]

    async def ensure_subenv_app_config(self, deployment_name: str, env: str) -> bool:
        if env == self.reference_env or env == "default" or env in app_config.excluded_namespaces:
            raise ValueError(f"Refusing to modify protected Apollo cluster {env}")
        synced = False
        # Shared aliases can produce concurrent events for the same Apollo app.
        async with self._lock:
            for app_id in self._resolve_target_app_ids(deployment_name):
                if app_id in self._skip_apps:
                    logger.info("Skip Apollo sync for app %s", app_id)
                    continue
                synced = await self._sync_single_app_config(app_id, env) or synced
        return synced

    def _copy_value(self, value, env):
        if ("https://sqs.ap-southeast-1.amazonaws.com/" in value
                or "arn:aws:sns:ap-southeast-1:" in value):
            return value
        return value.replace(self.reference_env.upper(), env.upper()).replace(self.reference_env, env)

    async def _sync_single_app_config(self, app_id: str, env: str) -> bool:
        source = self._cluster_path(app_id, self.reference_env)
        target = self._cluster_path(app_id, env)
        namespace = quote(f"web.{app_id}", safe="")
        suffix = f"/namespaces/{namespace}"
        async with self._session() as session:
            # Read only web.<app>; do not fetch secret Namespace items at all.
            reference = await self._request(session, "GET", source + suffix, missing_ok=True)
            if reference is None:
                logger.warning("No Apollo reference namespace for app=%s cluster=%s namespace=web.%s",
                               app_id, self.reference_env, app_id)
                return False
            if not isinstance(reference.get("items"), list):
                raise RuntimeError("Apollo reference namespace response is missing items")
            created = 0
            if await self._request(session, "GET", target, missing_ok=True) is None:
                try:
                    await self._request(session, "POST", target.rsplit("/", 1)[0], json={
                        "name": env, "appId": app_id, "dataChangeCreatedBy": self.operator,
                    })
                    created = 1
                except ApolloAPIError as error:
                    if error.status not in (400, 409):
                        raise
                    if await self._request(session, "GET", target, missing_ok=True) is None:
                        raise
            destination = await self._request(session, "GET", target + suffix, missing_ok=True)
            if destination is None:
                await self._ensure_web_namespace(app_id, env)
                destination = await self._request(session, "GET", target + suffix)
            if not isinstance(destination.get("items"), list):
                raise RuntimeError("Apollo target namespace response is missing items")
            existing = {item["key"] for item in destination.get("items", []) if item.get("key")}
            items_created = 0
            for item in reference.get("items", []):
                key = item.get("key")
                if not key or key in existing:
                    continue
                await self._request(session, "POST", target + suffix + "/items", json={
                    "key": key, "value": self._copy_value(item["value"], env),
                    "comment": item.get("comment") or "",
                    "dataChangeCreatedBy": self.operator,
                })
                items_created += 1
            latest = await self._request(session, "GET", target + suffix + "/releases/latest",
                                         missing_ok=True)
            released = 0
            if latest is None:
                await self._request(session, "POST", target + suffix + "/releases", json={
                    "releaseTitle": f"Initial release for {env}",
                    "releaseComment": "Created by namespace-watcher",
                    "releasedBy": self.operator,
                })
                released = 1
            elif items_created:
                # Do not publish an existing namespace's unrelated operator drafts.
                logger.warning("Apollo missing items copied but not published: app=%s cluster=%s; "
                               "existing release preserved, review and publish in Portal", app_id, env)
            logger.info("Apollo app config synced for app=%s env=%s clusters_created=%s "
                        "items_created=%s releases_created=%s", app_id, env, created, items_created, released)
        return True
