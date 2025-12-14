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
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.core.exceptions import ClientAuthenticationError


class FabricAuthenticator:
    """Handles authentication for Microsoft Fabric and Power BI APIs"""
    
    def __init__(self, tenant_id: str = None, client_id: str = None, client_secret: str = None):
        """
        Initialize the authenticator.
        
        Args:
            tenant_id: Azure tenant ID (optional)
            client_id: Application (client) ID (optional)
            client_secret: Client secret (optional)
            
        If credentials are not provided, attempts to use DefaultAzureCredential 
        or Fabric Notebook identity (notebookutils).
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
        
        # Initialize Credential Strategy
        if self.tenant_id and self.client_id and self.client_secret:
            masked_id = f"{self.client_id[:4]}...{self.client_id[-4:]}" if len(self.client_id) > 8 else "********"
            self.logger.info(f"Using Service Principal credentials (Client ID: {masked_id})")
            self.credential = ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret
            )
        else:
            self.logger.info("No Service Principal provided. Using DefaultAzureCredential/Notebook Identity.")
            self.credential = DefaultAzureCredential()
    
    def get_fabric_token(self, force_refresh: bool = False) -> str:
        """
        Get access token for Microsoft Fabric API.
        """
        if not force_refresh and self._is_token_valid(self._fabric_token_expires):
            return self._fabric_token
            
        # 1. Try Explicit Service Principal (Priority)
        if self.client_id and self.client_secret:
            self.logger.info("Acquiring Fabric API access token via Explicit Service Principal")
            # We do NOT catch exceptions here. If explicit credentials are provided but fail,
            # we should raise the error rather than silently falling back to a different identity.
            token = self.credential.get_token("https://api.fabric.microsoft.com/.default")
            
            self._fabric_token = token.token
            self._fabric_token_expires = datetime.fromtimestamp(token.expires_on)
            
            self.logger.info(f"Fabric token acquired, expires at: {self._fabric_token_expires}")
            return self._fabric_token

        # 2. Try notebookutils (Fabric Environment)
        try:
            from notebookutils import credentials
            self.logger.info("Acquiring token via notebookutils")
            token_str = credentials.getToken("pbi")
            self._fabric_token = token_str
            # notebookutils doesn't give expiry easily, assume valid for now or set short expiry
            self._fabric_token_expires = datetime.now() + timedelta(minutes=55) 
            return self._fabric_token
        except ImportError:
            pass # Not in Fabric notebook or notebookutils not available
            
        # 3. Try Azure Identity (Default/Managed Identity fallback)
        try:
            self.logger.info("Acquiring Fabric API access token via DefaultAzureCredential")
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
        """
        if not force_refresh and self._is_token_valid(self._powerbi_token_expires):
            return self._powerbi_token
            
        # 1. Try Explicit Service Principal (Priority)
        if self.client_id and self.client_secret:
            self.logger.info("Acquiring Power BI API access token via Explicit Service Principal")
            # We do NOT catch exceptions here. If explicit credentials are provided but fail,
            # we should raise the error rather than silently falling back to a different identity.
            token = self.credential.get_token("https://analysis.windows.net/powerbi/api/.default")
            
            self._powerbi_token = token.token
            self._powerbi_token_expires = datetime.fromtimestamp(token.expires_on)
            
            self.logger.info(f"Power BI token acquired, expires at: {self._powerbi_token_expires}")
            return self._powerbi_token

        # 2. Try notebookutils
        try:
            from notebookutils import credentials
            self.logger.info("Acquiring PowerBI token via notebookutils")
            token_str = credentials.getToken("pbi")
            self._powerbi_token = token_str
            self._powerbi_token_expires = datetime.now() + timedelta(minutes=55)
            return self._powerbi_token
        except ImportError:
            pass

        # 3. Try Azure Identity (Default/Managed Identity fallback)
        try:
            self.logger.info("Acquiring Power BI API access token via DefaultAzureCredential")
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
    
    Expected environment variables (preferred):
    - AZURE_TENANT_ID
    - AZURE_CLIENT_ID
    - AZURE_CLIENT_SECRET

    Backward-compatible aliases (optional):
    - TENANT_ID
    - CLIENT_ID
    - CLIENT_SECRET
    
    If variables are missing, returns an authenticator that uses DefaultAzureCredential.
    
    Returns:
        Configured FabricAuthenticator instance
    """
    tenant_id = os.getenv("AZURE_TENANT_ID") or os.getenv("TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID") or os.getenv("CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET") or os.getenv("CLIENT_SECRET")
    
    if not all([tenant_id, client_id, client_secret]):
        logging.getLogger(__name__).info("Environment variables for Service Principal not found. Using DefaultAzureCredential.")
        return FabricAuthenticator()
    
    return FabricAuthenticator(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )
