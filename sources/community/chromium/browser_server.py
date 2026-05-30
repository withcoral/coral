import os
import sys
import json
import sqlite3
import shutil
import tempfile
import re
from urllib.parse import urlparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

def get_base_path(browser):
    if sys.platform == "darwin":
        paths = {
            "chrome": "~/Library/Application Support/Google/Chrome",
            "edge": "~/Library/Application Support/Microsoft Edge",
            "brave": "~/Library/Application Support/BraveSoftware/Brave-Browser"
        }
    elif sys.platform == "win32":
        paths = {
            "chrome": "~\\AppData\\Local\\Google\\Chrome\\User Data",
            "edge": "~\\AppData\\Local\\Microsoft\\Edge\\User Data",
            "brave": "~\\AppData\\Local\\BraveSoftware\\Brave-Browser\\User Data"
        }
    else:
        paths = {
            "chrome": "~/.config/google-chrome",
            "edge": "~/.config/microsoft-edge",
            "brave": "~/.config/BraveSoftware/Brave-Browser"
        }
    return os.path.expanduser(paths.get(browser, paths["chrome"]))

def resolve_active_profile(browser):
    base_path = get_base_path(browser)
    if not os.path.exists(base_path):
        print(f"Base path not found for {browser}")
        return None

    profiles = []
    # Optimized: We only check immediate child directories instead of a deep os.walk
    for d in os.listdir(base_path):
        if d == "Default" or d.startswith("Profile ") or d.startswith("Guest Profile"):
            dir_path = os.path.join(base_path, d)
            if os.path.isdir(dir_path):
                history_path = os.path.join(dir_path, "History")
                bookmarks_path = os.path.join(dir_path, "Bookmarks")
                
                mtime = 0
                if os.path.exists(history_path):
                    mtime = max(mtime, os.path.getmtime(history_path))
                if os.path.exists(bookmarks_path):
                    mtime = max(mtime, os.path.getmtime(bookmarks_path))
                    
                if mtime > 0:
                    profiles.append((dir_path, mtime))

    if not profiles:
        default_path = os.path.join(base_path, "Default")
        return default_path if os.path.exists(default_path) else None

    # Deterministic profile selection passed down to functions
    profiles.sort(key=lambda x: x[1], reverse=True)
    best_match = profiles[0][0]
    print(f"Resolved active profile: {best_match}")
    return best_match

def query_sqlite(db_name, profile_path, query):
    original_path = os.path.join(profile_path, db_name)
    if not os.path.exists(original_path):
        return []
    
    temp_dir = tempfile.mkdtemp()
    temp_path = os.path.join(temp_dir, db_name)
    
    # Copying WAL and SHM sidecars
    for ext in ["", "-wal", "-shm"]:
        src = original_path + ext
        if os.path.exists(src):
            shutil.copy2(src, temp_path + ext)
            
    results = []
    try:
        conn = sqlite3.connect(temp_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            results.append(dict(row))
        conn.close()
    except Exception as e:
        print(f"SQLite Error reading {db_name}: {e}")
    finally:
        shutil.rmtree(temp_dir)
    return results

def extract_bookmarks(profile_path):
    path = os.path.join(profile_path, "Bookmarks")
    if not os.path.exists(path): 
        return []
    
    try:
        with open(path, "r", encoding="utf-8") as f: 
            data = json.load(f)
    except Exception as e:
        print(f"Error parsing JSON in {path}: {e}")
        return []
        
    results = []
    def traverse(node):
        if isinstance(node, list):
            for item in node:
                traverse(item)
        elif isinstance(node, dict):
            if "type" in node and "name" in node:
                results.append({
                    "id": str(node.get("id", "")), 
                    "title": node.get("name", ""),
                    "url": node.get("url", ""), 
                    "type": node.get("type", ""),
                    "date_added": str(node.get("date_added", ""))
                })
            if "children" in node:
                traverse(node["children"])
            for key, value in node.items():
                if isinstance(value, dict) and key != "children":
                    traverse(value)

    traverse(data.get("roots", {}))
    return results

def extract_history(profile_path):
    q = "SELECT id, url, title, visit_count, last_visit_time FROM urls ORDER BY last_visit_time DESC LIMIT 5000"
    return query_sqlite("History", profile_path, q)

def extract_downloads(profile_path):
    q = "SELECT id, target_path, received_bytes, total_bytes, start_time FROM downloads ORDER BY start_time DESC LIMIT 2000"
    return query_sqlite("History", profile_path, q)

def extract_top_sites(profile_path):
    q = "SELECT url_rank, url, title FROM top_sites ORDER BY url_rank ASC LIMIT 100"
    return query_sqlite("Top Sites", profile_path, q)

def extract_extensions(profile_path):
    ext_path = os.path.join(profile_path, "Extensions")
    if not os.path.exists(ext_path): 
        return []
    
    results = []
    for ext_id in os.listdir(ext_path):
        id_path = os.path.join(ext_path, ext_id)
        if not os.path.isdir(id_path): 
            continue
        
        versions = os.listdir(id_path)
        if not versions: 
            continue
            
        # Deterministic version selection
        versions.sort(key=lambda v: os.path.getmtime(os.path.join(id_path, v)), reverse=True)
        manifest_path = os.path.join(id_path, versions[0], "manifest.json")
        
        if os.path.exists(manifest_path):
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest = json.load(f)
                    name = manifest.get("name", "")
                    if name.startswith("__MSG_"): 
                        name = ext_id 
                    results.append({"id": ext_id, "name": name, "version": manifest.get("version", "")})
            except Exception as e:
                # Removed bare except
                print(f"Error reading extension manifest {manifest_path}: {e}")
    return results

def extract_tabs(profile_path):
    session_dir = os.path.join(profile_path, "Sessions")
    if not os.path.exists(session_dir): 
        return []
    
    urls = set()
    for file in os.listdir(session_dir):
        if file.startswith("Tabs_") or file.startswith("Session_"):
            file_path = os.path.join(session_dir, file)
            try:
                with open(file_path, "rb") as f:
                    content = f.read().decode('ascii', errors='ignore')
                    found_urls = re.findall(r'(https?://[^\s\x00]+)', content)
                    urls.update(found_urls)
            except Exception as e:
                # Removed bare except
                print(f"Error reading session file {file_path}: {e}")
    return [{"url": u} for u in list(urls)]

class BrowserAPIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        # Fixed URL parsing
        parsed_path = urlparse(self.path).path
        path_parts = parsed_path.strip("/").split("/")
        
        if len(path_parts) == 2:
            browser, data_type = path_parts
            if browser in ["chrome", "edge", "brave"]:
                profile_path = resolve_active_profile(browser)
                if not profile_path:
                    self.send_success({"data": []})
                    return
                    
                funcs = {
                    "bookmarks": extract_bookmarks, 
                    "history": extract_history,
                    "downloads": extract_downloads, 
                    "top_sites": extract_top_sites,
                    "extensions": extract_extensions, 
                    "tabs": extract_tabs
                }
                
                if data_type in funcs:
                    data = funcs[data_type](profile_path)
                    self.send_success({"data": data})
                    return
        
        self.send_response(404)
        self.end_headers()
        
    def send_success(self, data):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode("utf-8"))
        
    def log_message(self, format, *args):
        pass

if __name__ == "__main__":
    port = 8765
    # ThreadingHTTPServer implemented
    server = ThreadingHTTPServer(("127.0.0.1", port), BrowserAPIHandler)
    print(f"Starting server on http://127.0.0.1:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server.")
        server.server_close()
        sys.exit(0)