from supabase import create_client, Client
import os
from dotenv import load_dotenv

load_dotenv()

_supabase_client: Client | None = None
_supabase_url: str | None = None
_supabase_key: str | None = None

def _read_env() -> tuple[str, str]:
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY", "")
    return url, key

def _get_client() -> Client:
    global _supabase_client, _supabase_url, _supabase_key
    current_url, current_key = _read_env()
    if (
        _supabase_client is None
        or _supabase_url != current_url
        or _supabase_key != current_key
    ):
        _supabase_url = current_url
        _supabase_key = current_key
        _supabase_client = create_client(_supabase_url, _supabase_key)
    return _supabase_client

def get_supabase():
    return _get_client()
