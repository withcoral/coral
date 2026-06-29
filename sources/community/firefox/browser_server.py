
import os
import sys
import json
import secrets
import sqlite3
import shutil
import tempfile
import time
import configparser
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional

# ---------------------------------------------------------------------------
# Auth token – read from env or generate a fresh one at startup.
# The same value must be exported as FIREFOX_API_KEY before running
# `coral source add firefox` so the manifest can forward it in the
# Authorization header.
# ---------------------------------------------------------------------------
_SERVER_TOKEN: str = os.environ.get("FIREFOX_API_KEY") or secrets.token_hex(32)

_PROFILE_CACHE_TTL_SECONDS = 60
_DEFAULT_PORT = 8766
_MAX_HISTORY_LIMIT = 5000


def _read_server_config():
    base_url = os.environ.get("FIREFOX_BASE_URL")
    port_text = os.environ.get("FIREFOX_PORT", str(_DEFAULT_PORT))

    if base_url:
        base_url = base_url.rstrip("/")
        parsed = urlparse(base_url)
        if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost"}:
            raise ValueError("FIREFOX_BASE_URL must be http://127.0.0.1:<port> or http://localhost:<port>.")
        if parsed.port is None:
            raise ValueError("FIREFOX_BASE_URL must include an explicit port.")
        port = parsed.port
        if port < 1 or port > 65535:
            raise ValueError("FIREFOX_BASE_URL port must be between 1 and 65535.")
        if parsed.path not in {"", "/"}:
            raise ValueError("FIREFOX_BASE_URL must not include a path.")
        if parsed.params or parsed.query or parsed.fragment:
            raise ValueError("FIREFOX_BASE_URL must not include params, query, or fragment.")
        host_header = f"{parsed.hostname}:{port}"
        normalized_base_url = f"{parsed.scheme}://{host_header}"
        return port, parsed.hostname.lower(), host_header, normalized_base_url, normalized_base_url

    try:
        port = int(port_text)
    except ValueError as exc:
        raise ValueError("FIREFOX_PORT must be an integer.") from exc

    if port < 1 or port > 65535:
        raise ValueError("FIREFOX_PORT must be between 1 and 65535.")

    base_url = f"http://127.0.0.1:{port}"
    return port, "127.0.0.1", f"127.0.0.1:{port}", base_url, base_url


try:
    PORT, _BIND_HOST, _HOST_HEADER, _ALLOWED_ORIGIN, _BASE_URL = _read_server_config()
except ValueError as exc:
    print(f"Firefox source configuration error: {exc}", file=sys.stderr)
    sys.exit(2)

# ---------------------------------------------------------------------------
# PRTime helper
# ---------------------------------------------------------------------------

def convert_prtime(pr_time):
    if not pr_time:
        return None
    try:
        dt = datetime.fromtimestamp(int(pr_time) / 1000000.0, tz=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return str(pr_time)

# ---------------------------------------------------------------------------
# Profile resolution
# ---------------------------------------------------------------------------

def get_base_path():
    env_base_path = os.environ.get("FIREFOX_BASE_PATH") or os.environ.get("FIREFOX_PROFILES_PATH")
    if env_base_path:
        expanded = os.path.abspath(os.path.expanduser(env_base_path))
        if os.path.exists(os.path.join(expanded, "profiles.ini")):
            return expanded

        parent = os.path.dirname(expanded)
        if os.path.basename(expanded).lower() == "profiles" and os.path.exists(
            os.path.join(parent, "profiles.ini")
        ):
            return parent

        return expanded

    if sys.platform == "darwin":
        path = "~/Library/Application Support/Firefox"
    elif sys.platform == "win32":
        path = "~\\AppData\\Roaming\\Mozilla\\Firefox"
    else:
        path = "~/.mozilla/firefox"
    return os.path.expanduser(path)


def _has_places_database(path: str) -> bool:
    return os.path.exists(os.path.join(path, "places.sqlite"))


def _read_ini(path: str):
    if not os.path.exists(path):
        return None

    cfg = configparser.ConfigParser()
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg.read_file(f)
    except Exception as e:
        print(f"Warning: could not parse {path}: {e}")
        return None
    return cfg


def _profile_path_from_section(base_path: str, section: str, cfg: configparser.ConfigParser):
    path_val = cfg.get(section, "Path", fallback=None)
    if not path_val:
        return None

    is_relative = cfg.getboolean(section, "IsRelative", fallback=True)
    candidate = os.path.join(base_path, path_val) if is_relative else path_val
    return os.path.normpath(candidate)


def _profile_path_from_install_default(
    base_path: str,
    default_path: str,
    profile_cfg: configparser.ConfigParser,
):
    for section in profile_cfg.sections():
        if not section.lower().startswith("profile"):
            continue
        section_path = profile_cfg.get(section, "Path", fallback=None)
        if not section_path:
            continue
        if os.path.normcase(os.path.normpath(section_path)) == os.path.normcase(
            os.path.normpath(default_path)
        ):
            return _profile_path_from_section(base_path, section, profile_cfg)

    candidate = default_path if os.path.isabs(default_path) else os.path.join(base_path, default_path)
    return os.path.normpath(candidate)


def _collect_install_default_candidates(
    base_path: str,
    cfg: configparser.ConfigParser,
    installs_cfg: Optional[configparser.ConfigParser],
):
    candidates = {}

    def add_candidate(default_path: str):
        candidate = _profile_path_from_install_default(base_path, default_path, cfg)
        if _has_places_database(candidate):
            candidates[os.path.normcase(os.path.normpath(candidate))] = candidate

    for section in cfg.sections():
        if section.startswith("Install"):
            default_path = cfg.get(section, "Default", fallback=None)
            if default_path:
                add_candidate(default_path)

    if installs_cfg:
        for section in installs_cfg.sections():
            default_path = installs_cfg.get(section, "Default", fallback=None)
            if default_path:
                add_candidate(default_path)

    return list(candidates.values())


def _resolve_from_ini(base_path: str):
    """Parse Firefox's profiles.ini to find the default profile.

    Resolution order:
      1. A single unambiguous [Install...] default profile
      2. [Profile...] section with Default=1, only as a legacy fallback
      3. None - caller must require FIREFOX_PROFILE_PATH
    """
    cfg = _read_ini(os.path.join(base_path, "profiles.ini"))
    if cfg:
        # First check [Install...] sections (modern per-install defaults).
        install_candidates = _collect_install_default_candidates(
            base_path,
            cfg,
            _read_ini(os.path.join(base_path, "installs.ini")),
        )
        if len(install_candidates) == 1:
            candidate = install_candidates[0]
            print(f"Resolved profile via install default metadata: {candidate}")
            return candidate, None
        if len(install_candidates) > 1:
            return None, (
                "Multiple Firefox install defaults were found in profiles.ini/installs.ini. "
                "Set FIREFOX_PROFILE_PATH to the profile directory you want Coral to use."
            )

        # Fallback to [Profile...] with Default=1 (older format).
        for section in cfg.sections():
            if not section.lower().startswith("profile"):
                continue
            if cfg.get(section, "Default", fallback=None) != "1":
                continue
            candidate = _profile_path_from_section(base_path, section, cfg)
            if not candidate:
                continue
            if _has_places_database(candidate):
                print(f"Resolved profile via profiles.ini [{section}]: {candidate}")
                return candidate, None
            print(
                f"profiles.ini [{section}] Path={cfg.get(section, 'Path', fallback='')!r} "
                f"does not contain places.sqlite - skipping"
            )

    return None, (
        "Could not resolve the Firefox default profile from profiles.ini. "
        "Set FIREFOX_PROFILE_PATH to a profile directory containing places.sqlite."
    )


def resolve_active_profile():
    profile_path, error_message = _resolve_active_profile()
    if error_message:
        print(error_message)
    return profile_path


def _resolve_active_profile():
    # Highest priority: explicit env override
    env_profile_path = os.environ.get("FIREFOX_PROFILE_PATH")
    if env_profile_path:
        profile_path = os.path.abspath(os.path.expanduser(env_profile_path))
        places_path = os.path.join(profile_path, "places.sqlite")
        if os.path.exists(places_path):
            print(f"Using Firefox profile from FIREFOX_PROFILE_PATH: {profile_path}")
            return profile_path, None
        return None, f"FIREFOX_PROFILE_PATH does not contain places.sqlite: {profile_path}"

    base_path = get_base_path()
    if not os.path.exists(base_path):
        return None, f"Firefox base path not found: {base_path}"

    # Try profiles.ini first; otherwise require an explicit override.
    return _resolve_from_ini(base_path)

# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------

def query_sqlite(db_name, profile_path, query, params=()):
    original_path = os.path.join(profile_path, db_name)
    if not os.path.exists(original_path):
        raise FileNotFoundError(
            f"Missing Firefox database: {original_path}. "
            f"Verify FIREFOX_PROFILE_PATH or Firefox profile metadata."
        )

    temp_dir = tempfile.mkdtemp()
    temp_path = os.path.join(temp_dir, db_name)

    results = []
    source_conn = None
    conn = None
    try:
        source_uri = Path(original_path).resolve().as_uri() + "?mode=ro"
        source_conn = sqlite3.connect(source_uri, uri=True)
        conn = sqlite3.connect(temp_path)
        source_conn.backup(conn)
        source_conn.close()
        source_conn = None
        conn.close()
        conn = None

        conn = sqlite3.connect(temp_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(query, params)
        for row in cursor.fetchall():
            results.append(dict(row))
    except PermissionError as e:
        raise RuntimeError(
            f"Cannot read {db_name}: the file is locked. "
            "On Windows, close Firefox before querying Coral."
        ) from e
    except sqlite3.OperationalError as e:
        message = str(e).lower()
        if "locked" in message or "access" in message or "readonly" in message:
            raise RuntimeError(
                f"Cannot read {db_name}: the file is locked or unavailable. "
                "On Windows, close Firefox before querying Coral."
            ) from e
        raise RuntimeError(f"SQLite error reading {db_name}: {e}") from e
    except Exception as e:
        raise RuntimeError(f"SQLite error reading {db_name}: {e}") from e
    finally:
        if source_conn:
            source_conn.close()
        if conn:
            conn.close()
        shutil.rmtree(temp_dir, ignore_errors=True)
    return results

# ---------------------------------------------------------------------------
# Data extractors
# ---------------------------------------------------------------------------

def extract_bookmarks(profile_path):
    q = """
    SELECT b.id, b.guid, b.parent as parent_id, b.position,
           b.title, p.url, b.dateAdded as date_added,
           b.lastModified as last_modified, b.type
    FROM moz_bookmarks b
    LEFT JOIN moz_places p ON b.fk = p.id
    WHERE b.type IN (1, 2) AND b.title IS NOT NULL AND b.title != ''
    """
    results = query_sqlite("places.sqlite", profile_path, q)
    for r in results:
        r["date_added"] = convert_prtime(r["date_added"])
        r["last_modified"] = convert_prtime(r["last_modified"])
        r["type"] = "folder" if r["type"] == 2 else "url"
    return results


def _first_query_value(query_params, key):
    values = query_params.get(key) if query_params else None
    if not values:
        return None
    value = values[0]
    if value == "":
        return None
    return value


def _parse_limit(value, default):
    if value is None:
        return default
    try:
        limit = int(value)
    except ValueError as exc:
        raise ValueError("history limit must be an integer.") from exc
    if limit < 1 or limit > _MAX_HISTORY_LIMIT:
        raise ValueError(f"history limit must be between 1 and {_MAX_HISTORY_LIMIT}.")
    return limit


def _parse_history_time(value, key):
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        pass

    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{key} must be an ISO 8601 timestamp or Firefox PRTime microseconds.") from exc

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000000)


def _like_pattern(value):
    escaped = value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


def extract_history(profile_path, query_params=None):
    limit = _parse_limit(_first_query_value(query_params, "limit"), _MAX_HISTORY_LIMIT)
    after = _parse_history_time(_first_query_value(query_params, "after"), "after")
    before = _parse_history_time(_first_query_value(query_params, "before"), "before")
    url = _first_query_value(query_params, "url")
    title = _first_query_value(query_params, "title")

    where = ["visit_count > 0"]
    params = []
    if after is not None:
        where.append("last_visit_date >= ?")
        params.append(after)
    if before is not None:
        where.append("last_visit_date <= ?")
        params.append(before)
    if url is not None:
        where.append("url LIKE ? ESCAPE '\\'")
        params.append(_like_pattern(url))
    if title is not None:
        where.append("title LIKE ? ESCAPE '\\'")
        params.append(_like_pattern(title))

    params.append(limit)
    q = (
        "SELECT id, url, title, visit_count, last_visit_date "
        f"FROM moz_places WHERE {' AND '.join(where)} "
        "ORDER BY last_visit_date DESC LIMIT ?"
    )
    results = query_sqlite("places.sqlite", profile_path, q, params)
    for r in results:
        r["last_visit_date"] = convert_prtime(r["last_visit_date"])
    return results


def extract_top_sites(profile_path):
    q = "SELECT url, title FROM moz_places WHERE frecency > 0 ORDER BY frecency DESC LIMIT 100"
    results = query_sqlite("places.sqlite", profile_path, q)
    for index, row in enumerate(results, start=1):
        row["url_rank"] = index
    return results


def extract_extensions(profile_path):
    ext_path = os.path.join(profile_path, "extensions.json")
    if not os.path.exists(ext_path):
        return []

    results = []
    try:
        with open(ext_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        for addon in data.get("addons", []):
            if addon.get("type") == "extension":
                addon_id = addon.get("id", "")
                if not addon_id:
                    continue
                name = addon.get("defaultLocale", {}).get("name") or addon.get("name", "")
                results.append({
                    "id": addon_id,
                    "name": name,
                    "version": addon.get("version", "")
                })
    except Exception as e:
        raise RuntimeError(f"Error parsing extensions.json: {e}") from e
    return results

# ---------------------------------------------------------------------------
# Profile cache (refresh every 60 s)
# ---------------------------------------------------------------------------

_UNSET = object()
_cached_profile = _UNSET
_cached_profile_error = None
_cache_time = 0.0


def get_active_profile():
    global _cached_profile, _cached_profile_error, _cache_time
    now = time.time()
    if _cached_profile is _UNSET or (now - _cache_time > _PROFILE_CACHE_TTL_SECONDS):
        _cached_profile, _cached_profile_error = _resolve_active_profile()
        if _cached_profile_error:
            print(_cached_profile_error)
        _cache_time = now
    return _cached_profile


def get_active_profile_error():
    return _cached_profile_error

# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------

class BrowserAPIHandler(BaseHTTPRequestHandler):

    # ------------------------------------------------------------------
    # Security gate – called before any business logic
    # ------------------------------------------------------------------

    def _check_security(self) -> bool:
        """Return True if the request passes all security checks.
        Sends the appropriate error response and returns False on failure.
        """
        # 1. Host header – block DNS-rebinding attacks
        host = self.headers.get("Host", "")
        if host.lower() != _HOST_HEADER.lower():
            self._send_error(400, "Invalid Host header.")
            return False

        # 1b. Origin / Referer – reject browser requests from any other origin.
        origin = self.headers.get("Origin")
        if origin is not None and origin != _ALLOWED_ORIGIN:
            self._send_error(403, "Invalid Origin header.")
            return False

        referer = self.headers.get("Referer")
        if referer is not None:
            parsed_referer = urlparse(referer)
            referer_origin = f"{parsed_referer.scheme}://{parsed_referer.netloc}"
            if referer_origin != _ALLOWED_ORIGIN:
                self._send_error(403, "Invalid Referer header.")
                return False

        # 2. Sec-Fetch-Site – reject cross-site browser fetches
        #    Header is only sent by modern browsers; absence (e.g. curl, Coral
        #    HTTP backend) is fine.
        sec_fetch_site = self.headers.get("Sec-Fetch-Site")
        if sec_fetch_site is not None and sec_fetch_site not in {"none", "same-origin"}:
            self._send_error(403, "Cross-site requests are not allowed.")
            return False

        # 3. Bearer token
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            self._send_error(401, "Missing or invalid Authorization header.")
            return False
        provided_token = auth[len("Bearer "):]
        # Constant-time comparison to prevent timing attacks
        if not secrets.compare_digest(provided_token, _SERVER_TOKEN):
            self._send_error(401, "Invalid bearer token.")
            return False

        return True

    # ------------------------------------------------------------------
    # Request dispatch
    # ------------------------------------------------------------------

    def do_GET(self):
        if not self._check_security():
            return

        parsed_url = urlparse(self.path)
        path_parts = parsed_url.path.strip("/").split("/")

        if len(path_parts) == 2 and path_parts[0] == "firefox":
            data_type = path_parts[1]
            profile_path = get_active_profile()

            if not profile_path:
                error_message = get_active_profile_error() or (
                    "No Firefox profile found. "
                    "Set FIREFOX_PROFILE_PATH to a profile directory "
                    "containing places.sqlite."
                )
                self._send_error(
                    503,
                    error_message,
                )
                return

            funcs = {
                "bookmarks": extract_bookmarks,
                "history": extract_history,
                "history_slice": extract_history,
                "top_sites": extract_top_sites,
                "extensions": extract_extensions,
            }

            if data_type in funcs:
                try:
                    if data_type in {"history", "history_slice"}:
                        data = funcs[data_type](profile_path, parse_qs(parsed_url.query))
                    else:
                        data = funcs[data_type](profile_path)
                except ValueError as exc:
                    self._send_error(400, str(exc))
                    return
                except FileNotFoundError as exc:
                    self._send_error(503, str(exc))
                    return
                except RuntimeError as exc:
                    self._send_error(500, str(exc))
                    return

                self._send_json(200, {"data": data})
                return

        self._send_error(404, "Not found.")

    # ------------------------------------------------------------------
    # Response helpers
    # ------------------------------------------------------------------

    def _send_json(self, status: int, payload: dict):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, status: int, message: str):
        self._send_json(status, {"error": message})

    def log_message(self, format, *args):
        pass

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if "FIREFOX_API_KEY" not in os.environ:
        print("=" * 60)
        print("No FIREFOX_API_KEY set. Generated a new token:")
        print(f"  {_SERVER_TOKEN}")
        print()
        print("Export this value before running `coral source add firefox`:")
        print(f"  export FIREFOX_API_KEY={_SERVER_TOKEN}   # macOS/Linux")
        print(f"  $env:FIREFOX_API_KEY=\"{_SERVER_TOKEN}\"  # PowerShell")
        print("=" * 60)
    else:
        print("Using FIREFOX_API_KEY from environment.")

    ThreadingHTTPServer.allow_reuse_address = True
    server = ThreadingHTTPServer((_BIND_HOST, PORT), BrowserAPIHandler)
    print(f"Starting Firefox local server on {_BASE_URL}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server.")
        server.server_close()
        sys.exit(0)
