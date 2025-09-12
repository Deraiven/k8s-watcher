"""
Application configuration management
"""
import os
from typing import Optional
from dataclasses import dataclass


@dataclass
class AWSConfig:
    """AWS configuration"""
    region: str = os.getenv("AWS_REGION", "ap-southeast-1")
    access_key_id: Optional[str] = os.getenv("AWS_ACCESS_KEY_ID")
    secret_access_key: Optional[str] = os.getenv("AWS_SECRET_ACCESS_KEY")


@dataclass
class DatabaseConfig:
    """Database configuration"""
    host: str = os.getenv("MYSQL_HOST", "localhost")
    port: int = int(os.getenv("MYSQL_PORT", "3306"))
    user: str = os.getenv("MYSQL_USER", "apollo_user")
    password: Optional[str] = os.getenv("MYSQL_PASSWORD")
    database: str = os.getenv("MYSQL_DATABASE", "ApolloConfigDB_fat")


@dataclass
class KongConfig:
    """Kong API Gateway configuration"""
    admin_url: str = os.getenv("KONG_ADMIN_URL", "http://kong-kong-admin.proxy:8001")


@dataclass
class ZadigConfig:
    """Zadig configuration"""
    url: str = os.getenv("ZADIG_URL", "http://zadigx.shub.us")
    token: Optional[str] = os.getenv("ZADIG_TOKEN")
    project_key: str = os.getenv("ZADIG_PROJECT_KEY", "fat-base-envrionment")


@dataclass
class DNSConfig:
    """DNS Made Easy configuration"""
    api_key: Optional[str] = os.getenv("DNS_API_KEY")
    secret_key: Optional[str] = os.getenv("DNS_SECRET_KEY")
    api_url: str = "https://api.dnsmadeeasy.com/V2.0"


@dataclass
class CertManagerConfig:
    """cert-manager configuration"""
    namespace: str = os.getenv("CERT_MANAGER_NAMESPACE", "cert-manager")
    issuer_name: str = os.getenv("CERT_MANAGER_ISSUER", "letsencrypt-dnsmadeeasy-prod")
    issuer_kind: str = os.getenv("CERT_MANAGER_ISSUER_KIND", "ClusterIssuer")


@dataclass
class AppConfig:
    """Main application configuration"""
    reference_env: str = os.getenv("REFERENCE_ENV", "test33")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    in_cluster: bool = os.getenv("IN_CLUSTER", "true").lower() == "true"
    namespace_prefix: str = os.getenv("NAMESPACE_PREFIX", "test")
    namespace_label_key: str = os.getenv("NAMESPACE_LABEL_KEY", "createdBy")
    namespace_label_value: str = os.getenv("NAMESPACE_LABEL_VALUE", "koderover")
    
    # Feature flags
    enable_cert_management: bool = os.getenv("ENABLE_CERT_MANAGEMENT", "true").lower() == "true"
    enable_dns_management: bool = os.getenv("ENABLE_DNS_MANAGEMENT", "true").lower() == "true"
    enable_aws_resources: bool = os.getenv("ENABLE_AWS_RESOURCES", "true").lower() == "true"
    enable_kong_routes: bool = os.getenv("ENABLE_KONG_ROUTES", "true").lower() == "true"
    enable_apollo_config: bool = os.getenv("ENABLE_APOLLO_CONFIG", "false").lower() == "true"
    enable_zadig_workflow: bool = os.getenv("ENABLE_ZADIG_WORKFLOW", "true").lower() == "true"


# Global configuration instances
aws_config = AWSConfig()
db_config = DatabaseConfig()
kong_config = KongConfig()
zadig_config = ZadigConfig()
dns_config = DNSConfig()
cert_manager_config = CertManagerConfig()
app_config = AppConfig()