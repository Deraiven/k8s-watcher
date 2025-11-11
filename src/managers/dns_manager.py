"""
DNS Made Easy manager
"""
import hmac
import hashlib
import datetime
import json
from typing import Dict, List, Optional
import aiohttp

from ..config.settings import cloudflare_config

logger = setup_logger(__name__)


class CloudflareDNSManager:
    """Manager for Cloudflare DNS operations"""
    
    def __init__(self):
        self.api_token = cloudflare_config.api_token
        self.zone_id = cloudflare_config.zone_id
        self.api_url = cloudflare_config.api_url
        self.domain_name = "shub.us" # Base domain for record creation
    
    def _create_headers(self) -> Dict[str, str]:
        """Create authentication headers for Cloudflare API"""
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_token}",
        }
    
    async def _get_zone_id(self) -> Optional[str]:
        """Get Cloudflare Zone ID"""
        if not self.zone_id:
            logger.error("Cloudflare Zone ID is not configured.")
            return None
        return self.zone_id
    
    @async_retry(max_tries=3, exceptions=(aiohttp.ClientError, Exception))
    async def create_dns_record(self, env: str) -> bool:
        """Create DNS CNAME record for the environment using Cloudflare"""
        zone_id = await self._get_zone_id()
        if not zone_id:
            return False
        
        record_name = f'*.{env}.{self.domain_name}'
        record_content = 'aa1bec7defbef41e1aecbc587e423673-46828627eed20054.elb.ap-southeast-1.amazonaws.com.'
        
        record = {
            'type': 'CNAME',
            'name': record_name,
            'content': record_content,
            'ttl': 1,  # 1 means automatic
            'proxied': False
        }
        
        async with aiohttp.ClientSession() as session:
            headers = self._create_headers()
            
            try:
                async with session.post(
                    f"{self.api_url}/zones/{zone_id}/dns_records",
                    headers=headers,
                    data=json.dumps(record)
                ) as response:
                    response_json = await response.json()
                    if response.status == 200 and response_json.get('success'):
                        logger.info(f"Created DNS record: {record_name}")
                        return True
                    elif response.status == 400 and any('record already exists' in err.get('message', '').lower() for err in response_json.get('errors', [])):
                        logger.info(f"DNS record already exists: {record_name}, skipping")
                        return True
                    else:
                        error_message = response_json.get('errors', [{'message': 'Unknown error'}])[0].get('message')
                        raise Exception(f"Cloudflare API failed to create DNS record: {error_message}")
                    
            except aiohttp.ClientError as e:
                logger.error(f"Failed to create DNS record: {e}")
                raise
    
    @async_retry(max_tries=3, exceptions=(aiohttp.ClientError, Exception))
    async def delete_dns_record(self, env: str) -> bool:
        """Delete DNS record for the environment using Cloudflare"""
        zone_id = await self._get_zone_id()
        if not zone_id:
            return False
        
        record_name = f'*.{env}.{self.domain_name}'
        
        async with aiohttp.ClientSession() as session:
            headers = self._create_headers()
            
            try:
                # Find the record ID first
                async with session.get(
                    f"{self.api_url}/zones/{zone_id}/dns_records?type=CNAME&name={record_name}",
                    headers=headers
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    record_id = None
                    if data.get('success') and data.get('result'):
                        for record in data['result']:
                            if record['name'] == record_name:
                                record_id = record['id']
                                break
                    
                    if not record_id:
                        logger.info(f"DNS record not found: {record_name}, skipping deletion")
                        return True
                
                # Delete the record
                async with session.delete(
                    f"{self.api_url}/zones/{zone_id}/dns_records/{record_id}",
                    headers=headers
                ) as delete_response:
                    delete_response_json = await delete_response.json()
                    if delete_response.status == 200 and delete_response_json.get('success'):
                        logger.info(f"Deleted DNS record: {record_name}")
                        return True
                    else:
                        error_message = delete_response_json.get('errors', [{'message': 'Unknown error'}])[0].get('message')
                        raise Exception(f"Cloudflare API failed to delete DNS record: {error_message}")
                    
            except aiohttp.ClientError as e:
                logger.error(f"Failed to delete DNS record: {e}")
                raise