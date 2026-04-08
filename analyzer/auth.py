"""Persistent authentication for Fabric API.

Tries AzureCliCredential first (if `az login` is active), then falls back
to InteractiveBrowserCredential with a persistent MSAL token cache so the
browser popup only appears once per ~24h.
"""

import sys
import os
import json
import shutil
import subprocess
import time
from collections import namedtuple
from azure.identity import (
    AzureCliCredential,
    InteractiveBrowserCredential,
    TokenCachePersistenceOptions,
)

# SDK bootstrap — the SDK is installed to a temp directory
sys.path.insert(0, os.path.join(os.environ.get("TEMP", "/tmp"), "fabric_data_agent_client"))
from fabric_data_agent_client import FabricDataAgentClient

FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
FABRIC_RESOURCE = "https://api.fabric.microsoft.com"

_TokenInfo = namedtuple("_TokenInfo", ["token", "expires_on"])


class _AzCliShellCredential:
    """Fallback credential that calls `az` via shell=True (Windows .cmd compat)."""

    def __init__(self, tenant_id=None):
        self.tenant_id = tenant_id

    def get_token(self, *scopes, **kwargs):
        cmd = f'az account get-access-token --resource {FABRIC_RESOURCE} -o json'
        if self.tenant_id:
            cmd += f' --tenant {self.tenant_id}'
        r = subprocess.run(cmd, capture_output=True, text=True, shell=True, timeout=30)
        if r.returncode != 0:
            raise Exception(f"az CLI token failed: {r.stderr[:200]}")
        data = json.loads(r.stdout)
        from datetime import datetime
        exp = datetime.fromisoformat(data["expiresOn"].replace(" ", "T"))
        return _TokenInfo(token=data["accessToken"], expires_on=exp.timestamp())


def _get_credential(tenant_id):
    """Try AzureCliCredential first, shell fallback, then InteractiveBrowserCredential."""
    # Try standard SDK credential
    cli_cred = AzureCliCredential(tenant_id=tenant_id, process_timeout=30)
    try:
        cli_cred.get_token(FABRIC_SCOPE)
        print("  Auth: using Azure CLI credential")
        return cli_cred
    except Exception:
        pass

    # Windows fallback: use shell=True to invoke az.cmd
    try:
        shell_cred = _AzCliShellCredential(tenant_id=tenant_id)
        shell_cred.get_token(FABRIC_SCOPE)
        print("  Auth: using Azure CLI credential (shell fallback)")
        return shell_cred
    except Exception as e:
        print(f"  AzureCliCredential fallback failed: {e}")

    print("  Auth: Azure CLI not available, using browser login")
    cache_opts = TokenCachePersistenceOptions(
        name="ai_skill_analyzer",
        allow_unencrypted_storage=True,
    )
    return InteractiveBrowserCredential(
        tenant_id=tenant_id,
        cache_persistence_options=cache_opts,
    )


class FabricSession:
    """Wraps SDK client + REST token. Created once per invocation.

    Uses a shared persistent credential so the browser popup only appears
    once (or when the refresh token expires, typically after 24 h).
    """

    def __init__(self, cfg):
        self.cfg = cfg
        self._client = None
        self._token = None
        self._credential = _get_credential(cfg["tenant_id"])

    def _ensure_token(self):
        if self._token is None or self._token.expires_on <= time.time() + 300:
            self._token = self._credential.get_token(FABRIC_SCOPE)
            print(f"  Token valid until {time.ctime(self._token.expires_on)}")

    @property
    def client(self):
        if self._client is None:
            self._ensure_token()
            self._client = FabricDataAgentClient.__new__(FabricDataAgentClient)
            self._client.tenant_id = self.cfg["tenant_id"]
            self._client.data_agent_url = self.cfg["data_agent_url"]
            self._client.stage = self.cfg.get("stage", "production")
            self._client.api_version = self.cfg.get("api_version", FabricDataAgentClient.DEFAULT_API_VERSION)
            self._client.credential = self._credential
            self._client.token = self._token
            print(f"Fabric Data Agent Client ready (cached auth)")
            print(f"  Data Agent URL: {self.cfg['data_agent_url']}")
        self._ensure_token()
        self._client.token = self._token
        return self._client

    @property
    def token(self):
        self._ensure_token()
        return self._token.token

    @property
    def headers(self):
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
