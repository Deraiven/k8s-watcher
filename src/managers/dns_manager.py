"""
DNS Made Easy manager
"""
import hmac
import hashlib
import datetime
import json
from typing import Dict, List, Optional
import aiohttp

from ..config.settings import dns_config
from ..utils.logger import setup_logger
from ..utils.retry import async_retry

logger = setup_logger(__name__)


class DNSManager:
    """Manager for DNS Made Easy operations"""
    
    def __init__(self):
        self.api_key = dns_config.api_key
        self.secret_key = dns_config.secret_key
        self.api_url = dns_config.api_url
        self.domain_name = "shub.us"
        self.domain_id = None
    
    def _create_headers(self) -> Dict[str, str]:
        """Create authentication headers for DNS Made Easy API"""
        request_date = datetime.datetime.strftime(
            datetime.datetime.utcnow(), 
            '%a, %d %b %Y %H:%M:%S GMT'
        )
        
        hmac_text = hmac.new(
            bytes(self.secret_key, "UTF-8"), 
            bytes(request_date, "UTF-8"), 
            hashlib.sha1
        ).hexdigest()
        
        return {
            "Content-Type": "application/json",
            "x-dnsme-hmac": hmac_text,
            "x-dnsme-apiKey": self.api_key,
            "x-dnsme-requestDate": request_date,
        }
    
    async def _get_domain_id(self) -> Optional[str]:
        """Get domain ID for shub.us"""
        if self.domain_id:
            return self.domain_id
        
        async with aiohttp.ClientSession() as session:
            headers = self._create_headers()
            
            try:
                async with session.get(
                    f"{self.api_url}/dns/managed",
                    headers=headers
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    for domain in data['data']:
                        if domain['name'] == self.domain_name:
                            self.domain_id = domain['id']
                            return self.domain_id
                    
                    logger.error(f"Domain {self.domain_name} not found")
                    return None
                    
            except aiohttp.ClientError as e:
                logger.error(f"Failed to get domains: {e}")
                return None
    
    @async_retry(max_tries=3, exceptions=(aiohttp.ClientError,))
    async def create_dns_record(self, env: str) -> bool:
        """Create DNS CNAME record for the environment"""
        domain_id = await self._get_domain_id()
        if not domain_id:
            return False
        
        record = {
            'gtdLocation': 'DEFAULT',
            'ttl': 86400,
            'name': f'*.{env}',
            'value': 'aa1bec7defbef41e1aecbc587e423673-46828627eed20054.elb.ap-southeast-1.amazonaws.com.',
            'type': 'CNAME'
        }
        
        async with aiohttp.ClientSession() as session:
            headers = self._create_headers()
            
            try:
                async with session.post(
                    f"{self.api_url}/dns/managed/{domain_id}/records/",
                    headers=headers,
                    data=json.dumps(record)
                ) as response:
                    if response.status == 201:
                        logger.info(f"Created DNS record: {record['name']}.{self.domain_name}")
                        return True
                    elif response.status == 400:
                        error_data = await response.json()
                        error_text = str(error_data)
                        if 'already exists' in error_text or 'duplicate' in error_text.lower():
                            logger.info(f"DNS record already exists: {record['name']}.{self.domain_name}, skipping")
                            return True
                    
                    response.raise_for_status()
                    return False
                    
            except aiohttp.ClientError as e:
                logger.error(f"Failed to create DNS record: {e}")
                raise
    
    @async_retry(max_tries=3, exceptions=(aiohttp.ClientError,))
    async def delete_dns_record(self, env: str) -> bool:
        """Delete DNS record for the environment"""
        domain_id = await self._get_domain_id()
        if not domain_id:
            return False
        
        record_name = f"*.{env}"
        
        async with aiohttp.ClientSession() as session:
            headers = self._create_headers()
            
            try:
                # Get all records
                async with session.get(
                    f"{self.api_url}/dns/managed/{domain_id}/records",
                    headers=headers
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    # Find and delete the record
                    for record in data['data']:
                        if record['name'] == record_name:
                            delete_headers = self._create_headers()
                            async with session.delete(
                                f"{self.api_url}/dns/managed/{domain_id}/records/{record['id']}",
                                headers=delete_headers
                            ) as delete_response:
                                delete_response.raise_for_status()
                                logger.info(f"Deleted DNS record: {record_name}.{self.domain_name}")
                                return True
                    
                    logger.warning(f"DNS record not found: {record_name}.{self.domain_name}")
                    return True
                    
            except aiohttp.ClientError as e:
                logger.error(f"Failed to delete DNS record: {e}")
                raise