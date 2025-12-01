"""
Microsoft Fabric Authentication Module

This module handles authentication with Microsoft Fabric APIs using service principal credentials.
Supports both Azure Identity and direct OAuth2 flows.
"""

import os
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timedelta

import requests
from azure.identity import ClientSecretCredential
from azure.core.exceptions import ClientAuthenticationError


class FabricAuthenticator:
    """Handles authentication for Microsoft Fabric and Power BI APIs"""
    
    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        """
        Initialize the authenticator with service principal credentials.
        
        Args:
            tenant_id: Azure tenant ID
            client_id: Application (client) ID from app registration
            client_secret: Client secret from app registration
        """
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.logger = logging.getLogger(__name__)
        
        # Token cache
        self._fabric_token = None
        self._powerbi_token = None
        self._fabric_token_expires = None
        self._powerbi_token_expires = None
        
        # Initialize Azure credential
        self.credential = ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret
        )
    
    def get_fabric_token(self, force_refresh: bool = False) -> str:
        """
        Get access token for Microsoft Fabric API.
        
        Args:
            force_refresh: Force token refresh even if cached token is valid
            
        Returns:
            Bearer token for Fabric API
            
        Raises:
            ClientAuthenticationError: If authentication fails
        """
        if not force_refresh and self._is_token_valid(self._fabric_token_expires):
            return self._fabric_token
            
        try:
            self.logger.info("Acquiring Fabric API access token")
            token = self.credential.get_token("https://api.fabric.microsoft.com/.default")
            
            self._fabric_token = token.token
            self._fabric_token_expires = datetime.fromtimestamp(token.expires_on)
            
            self.logger.info(f"Fabric token acquired, expires at: {self._fabric_token_expires}")
            return self._fabric_token
            
        except Exception as e:
            self.logger.error(f"Failed to acquire Fabric token: {str(e)}")
            raise ClientAuthenticationError(f"Fabric authentication failed: {str(e)}")
    
    def get_powerbi_token(self, force_refresh: bool = False) -> str:
        """
        Get access token for Power BI API.
        
        Args:
            force_refresh: Force token refresh even if cached token is valid
            
        Returns:
            Bearer token for Power BI API
            
        Raises:
            ClientAuthenticationError: If authentication fails
        """
        if not force_refresh and self._is_token_valid(self._powerbi_token_expires):
            return self._powerbi_token
            
        try:
            self.logger.info("Acquiring Power BI API access token")
            token = self.credential.get_token("https://analysis.windows.net/powerbi/api/.default")
            
            self._powerbi_token = token.token
            self._powerbi_token_expires = datetime.fromtimestamp(token.expires_on)
            
            self.logger.info(f"Power BI token acquired, expires at: {self._powerbi_token_expires}")
            return self._powerbi_token
            
        except Exception as e:
            self.logger.error(f"Failed to acquire Power BI token: {str(e)}")
            raise ClientAuthenticationError(f"Power BI authentication failed: {str(e)}")
    
    def get_fabric_headers(self) -> Dict[str, str]:
        """Get HTTP headers for Fabric API requests"""
        token = self.get_fabric_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    def get_powerbi_headers(self) -> Dict[str, str]:
        """Get HTTP headers for Power BI API requests"""
        token = self.get_powerbi_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    def _is_token_valid(self, expires_at: Optional[datetime]) -> bool:
        """Check if token is still valid (with 5 minute buffer)"""
        if expires_at is None:
            return False
        return datetime.now() < (expires_at - timedelta(minutes=5))
    
    def validate_credentials(self) -> bool:
        """
        Validate that the service principal credentials work.
        
        Returns:
            True if credentials are valid, False otherwise
        """
        try:
            # Try to get both tokens
            fabric_token = self.get_fabric_token(force_refresh=True)
            powerbi_token = self.get_powerbi_token(force_refresh=True)
            
            if fabric_token and powerbi_token:
                self.logger.info("Service principal credentials validated successfully")
                return True
            else:
                self.logger.error("Failed to acquire one or more tokens")
                return False
                
        except Exception as e:
            self.logger.error(f"Credential validation failed: {str(e)}")
            return False


def create_authenticator_from_env() -> FabricAuthenticator:
    """
    Create authenticator from environment variables.
    
    Expected environment variables:
    - AZURE_TENANT_ID
    - AZURE_CLIENT_ID  
    - AZURE_CLIENT_SECRET
    
    Returns:
        Configured FabricAuthenticator instance
        
    Raises:
        ValueError: If required environment variables are missing
    """
    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    
    if not all([tenant_id, client_id, client_secret]):
        missing = []
        if not tenant_id:
            missing.append("AZURE_TENANT_ID")
        if not client_id:
            missing.append("AZURE_CLIENT_ID")
        if not client_secret:
            missing.append("AZURE_CLIENT_SECRET")
        
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    return FabricAuthenticator(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )
