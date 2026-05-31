import json
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import time
import secrets
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse


BROWSERS = {
    "chrome": {
        "display": "Google Chrome",
        "profile_env": "CHROME_PROFILE_PATH",
        "paths": {
            "darwin": "~/Library/Application Support/Google/Chrome",
            "win32": "~\\AppData\\Local\\Google\\Chrome\\User Data",
            "linux": "~/.config/google-chrome",
        },
    },
    "edge": {
        "display": "Microsoft Edge",
        "profile_env": "EDGE_PROFILE_PATH",
        "paths": {
            "darwin": "~/Library/Application Support/Microsoft Edge",
            "win32": "~\\AppData\\Local\\Microsoft\\Edge\\User Data",
            "linux": "~/.config/microsoft-edge",
        },
    },
    "brave": {
        "display": "Brave Browser",
        "profile_env": "BRAVE_PROFILE_PATH",
        "paths": {
            "darwin": "~/Library/Application Support/BraveSoftware/Brave-Browser",
            "win32": "~\\AppData\\Local\\BraveSoftware\\Brave-Browser\\User Data",
            "linux": "~/.config/BraveSoftware/Brave-Browser",
        },
    },
}

CACHE_TTL = 60
MAX_HISTORY_ROWS = 5000
MAX_DOWNLOAD_ROWS = 2000
_cached_profiles = {browser: None for browser in BROWSERS}
_cache_times = {browser: 0.0 for browser in BROWSERS}
_cache_errors = {browser: "" for browser in BROWSERS}


def convert_webkit_timestamp(webkit_timestamp):
    if not webkit_timestamp:
        return None
    try:
        epoch_start = datetime(1601, 1, 1, tzinfo=timezone.utc)
        converted = epoch_start + timedelta(microseconds=int(webkit_timestamp))
        return converted.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def get_base_path(browser):
    platform_key = "linux"
    if sys.platform == "darwin":
        platform_key = "darwin"
    elif sys.platform == "win32":
        platform_key = "win32"
    return os.path.expanduser(BROWSERS[browser]["paths"][platform_key])


def _is_profile_dir(path):
    return os.path.isdir(path) and (
        os.path.exists(os.path.join(path, "History"))
        or os.path.exists(os.path.join(path, "Bookmarks"))
        or os.path.exists(os.path.join(path, "Preferences"))
    )


def _resolve_env_profile(browser):
    env_name = BROWSERS[browser]["profile_env"]
    configured = os.environ.get(env_name)
    if not configured:
        return None, ""

    profile_path = os.path.abspath(os.path.expanduser(configured))
    if _is_profile_dir(profile_path):
        return profile_path, ""
    return None, f"{env_name} does not point to a readable Chromium profile directory: {profile_path}"


def _discover_profiles(base_path):
    profiles = []
    try:
        for name in os.listdir(base_path):
            profile_path = os.path.join(base_path, name)
            if _is_profile_dir(profile_path):
                profiles.append(name)
    except OSError:
        return []
    return sorted(profiles)


def _resolve_from_local_state(browser):
    base_path = get_base_path(browser)
    if not os.path.isdir(base_path):
        return None, (
            f"{BROWSERS[browser]['display']} profile root not found. "
            f"Set {BROWSERS[browser]['profile_env']} to the full profile directory."
        )

    local_state_path = os.path.join(base_path, "Local State")
    try:
        with open(local_state_path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {}
    except (json.JSONDecodeError, OSError) as exc:
        return None, f"Could not read Chromium Local State at {local_state_path}: {exc}"

    last_used = data.get("profile", {}).get("last_used")
    if last_used:
        candidate = os.path.join(base_path, last_used)
        if _is_profile_dir(candidate):
            return candidate, ""
        return None, (
            f"Local State points to profile '{last_used}', but that profile could not be read. "
            f"Set {BROWSERS[browser]['profile_env']} to the full profile directory."
        )

    profiles = _discover_profiles(base_path)
    if len(profiles) == 1:
        return os.path.join(base_path, profiles[0]), ""
    if len(profiles) > 1:
        return None, (
            f"Multiple {BROWSERS[browser]['display']} profiles found. "
            f"Set {BROWSERS[browser]['profile_env']} to the full profile directory you want to query "
            f"(for example, {os.path.join(base_path, profiles[0])})."
        )
    return None, (
        f"{BROWSERS[browser]['display']} profile not found. "
        f"Set {BROWSERS[browser]['profile_env']} to the full profile directory."
    )


def _resolve_active_profile(browser):
    env_profile, env_error = _resolve_env_profile(browser)
    if env_profile:
        return env_profile, ""
    local_state_profile, local_state_error = _resolve_from_local_state(browser)
    if local_state_profile:
        return local_state_profile, ""
    return None, env_error or local_state_error


def get_active_profile(browser):
    now = time.time()
    if now - _cache_times[browser] > CACHE_TTL:
        profile_path, error = _resolve_active_profile(browser)
        _cached_profiles[browser] = profile_path
        _cache_errors[browser] = error
        _cache_times[browser] = now
    return _cached_profiles[browser], _cache_errors[browser]


def query_sqlite(db_name, profile_path, query):
    original_path = os.path.join(profile_path, db_name)
    if not os.path.exists(original_path):
        return []

    temp_dir = tempfile.mkdtemp()
    temp_path = os.path.join(temp_dir, db_name)
    conn = None
    try:
        for ext in ["", "-wal", "-shm"]:
            src = original_path + ext
            if os.path.exists(src):
                shutil.copy2(src, temp_path + ext)

        conn = sqlite3.connect(temp_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]
    except Exception as exc:
        print(f"SQLite error reading {db_name}: {exc}", file=sys.stderr)
        return []
    finally:
        if conn is not None:
            conn.close()
        shutil.rmtree(temp_dir, ignore_errors=True)


def extract_bookmarks(profile_path):
    path = os.path.join(profile_path, "Bookmarks")
    if not os.path.exists(path):
        return []

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        print(f"Error parsing JSON in {path}: {exc}", file=sys.stderr)
        return []

    results = []

    def traverse(node):
        if isinstance(node, list):
            for item in node:
                traverse(item)
        elif isinstance(node, dict):
            if "type" in node and "name" in node:
                results.append(
                    {
                        "id": str(node.get("id", "")),
                        "title": node.get("name", ""),
                        "url": node.get("url", ""),
                        "type": node.get("type", ""),
                        "date_added": convert_webkit_timestamp(node.get("date_added")),
                    }
                )
            if "children" in node:
                traverse(node["children"])
            for key, value in node.items():
                if isinstance(value, dict) and key != "children":
                    traverse(value)

    traverse(data.get("roots", {}))
    return results


def extract_history(profile_path):
    query = (
        "SELECT id, url, title, visit_count, last_visit_time "
        f"FROM urls ORDER BY last_visit_time DESC LIMIT {MAX_HISTORY_ROWS}"
    )
    results = query_sqlite("History", profile_path, query)
    for row in results:
        row["last_visit_time"] = convert_webkit_timestamp(row["last_visit_time"])
    return results


def extract_downloads(profile_path):
    query = (
        "SELECT id, target_path, received_bytes, total_bytes, start_time "
        f"FROM downloads ORDER BY start_time DESC LIMIT {MAX_DOWNLOAD_ROWS}"
    )
    results = query_sqlite("History", profile_path, query)
    for row in results:
        row["start_time"] = convert_webkit_timestamp(row["start_time"])
    return results


def extract_top_sites(profile_path):
    query = "SELECT url_rank, url, title FROM top_sites ORDER BY url_rank ASC LIMIT 100"
    return query_sqlite("Top Sites", profile_path, query)


def extract_extensions(profile_path):
    ext_path = os.path.join(profile_path, "Extensions")
    if not os.path.exists(ext_path):
        return []

    results = []
    for ext_id in os.listdir(ext_path):
        id_path = os.path.join(ext_path, ext_id)
        if not os.path.isdir(id_path):
            continue

        versions = [v for v in os.listdir(id_path) if os.path.isdir(os.path.join(id_path, v))]
        if not versions:
            continue

        versions.sort(key=lambda v: os.path.getmtime(os.path.join(id_path, v)), reverse=True)
        manifest_path = os.path.join(id_path, versions[0], "manifest.json")
        if not os.path.exists(manifest_path):
            continue

        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
            name = manifest.get("name", "")
            if name.startswith("__MSG_"):
                name = ext_id
            results.append({"id": ext_id, "name": name, "version": manifest.get("version", "")})
        except Exception as exc:
            print(f"Error reading extension manifest {manifest_path}: {exc}", file=sys.stderr)
    return results


def extract_tabs(profile_path):
    session_dir = os.path.join(profile_path, "Sessions")
    if not os.path.exists(session_dir):
        return []

    urls = set()
    for name in os.listdir(session_dir):
        if not name.startswith(("Tabs_", "Session_")):
            continue
        file_path = os.path.join(session_dir, name)
        try:
            with open(file_path, "rb") as f:
                content = f.read().decode("ascii", errors="ignore")
            urls.update(re.findall(r"https?://[^\s\x00]+", content))
        except Exception as exc:
            print(f"Error reading session file {file_path}: {exc}", file=sys.stderr)
    return [{"url": url} for url in sorted(urls)]


class BrowserAPIHandler(BaseHTTPRequestHandler):
    server_version = "ChromiumLocalSource/0.1"

    def _check_auth(self):
        expected_token = os.environ.get("CHROME_API_KEY")
        if not expected_token:
            self.send_error(503, "CHROME_API_KEY is not set on the local server.")
            return False

        expected_hosts = {
            f"127.0.0.1:{self.server.server_port}",
            f"localhost:{self.server.server_port}",
        }
        if self.headers.get("Host") not in expected_hosts:
            self.send_error(400, "Unexpected Host header.")
            return False

        origin = self.headers.get("Origin")
        allowed_origins = {f"http://{host}" for host in expected_hosts}
        if origin and origin not in allowed_origins:
            self.send_error(403, "Unexpected Origin header.")
            return False

        sec_fetch_site = self.headers.get("Sec-Fetch-Site")
        if sec_fetch_site and sec_fetch_site not in {"same-origin", "same-site", "none"}:
            self.send_error(403, "Unexpected Sec-Fetch-Site header.")
            return False

        auth = self.headers.get("Authorization", "")
        if auth != f"Bearer {expected_token}":
            self.send_error(401, "Unauthorized.")
            return False
        return True

    def do_GET(self):
        if not self._check_auth():
            return

        parsed_path = urlparse(self.path).path
        path_parts = parsed_path.strip("/").split("/")
        if len(path_parts) != 2:
            self.send_error(404, "Not found.")
            return

        browser, data_type = path_parts
        if browser not in BROWSERS:
            self.send_error(404, "Unknown browser.")
            return

        profile_path, profile_error = get_active_profile(browser)
        if profile_path is None:
            self.send_error(503, profile_error)
            return

        funcs = {
            "bookmarks": extract_bookmarks,
            "history": extract_history,
            "downloads": extract_downloads,
            "top_sites": extract_top_sites,
            "extensions": extract_extensions,
            "tabs": extract_tabs,
        }
        if data_type not in funcs:
            self.send_error(404, "Unknown table.")
            return

        data = funcs[data_type](profile_path)
        self.send_success({"data": data})

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        super().end_headers()

    def send_success(self, data):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode("utf-8"))

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    # Fixed port per project policy
    port = 8765
    server = ThreadingHTTPServer(("127.0.0.1", port), BrowserAPIHandler)

    # If operator hasn't set CHROME_API_KEY, generate one and print instructions
    token = os.environ.get("CHROME_API_KEY")
    if not token:
        token = secrets.token_hex(32)
        print("No CHROME_API_KEY found. Generated one for you:")
        print("")
        print(f"API token: {token}")
        print("")
        print("Set this token in the shell where you run Coral commands:")
        print("PowerShell (temporary for current session):")
        print(f"  $env:CHROME_API_KEY = \"{token}\"")
        print("PowerShell (persist across sessions):")
        print(f"  setx CHROME_API_KEY {token}")
        print("bash/zsh:")
        print(f"  export CHROME_API_KEY={token}")
        print("")
        print("Start Coral commands in a shell that has CHROME_API_KEY set to the above value.")
        # Make the generated token available to this process for auth checks
        os.environ["CHROME_API_KEY"] = token
    else:
        print("Using CHROME_API_KEY from environment.")

    print(f"Starting Chromium local source server on http://127.0.0.1:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server.")
        server.server_close()
        sys.exit(0)
