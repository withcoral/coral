import os
import sys
import json
import sqlite3
import shutil
import tempfile
import re
from http.server import BaseHTTPRequestHandler, HTTPServer

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

def get_target(browser, target_name, is_dir=False):
    base_path = get_base_path(browser)
    if not os.path.exists(base_path):
        print(f"[-] Base path not found for {browser}: {base_path}")
        return None

    candidates = []
    for root, dirs, files in os.walk(base_path):
        if "Cache" in root or "System Profile" in root or "Crashpad" in root:
            continue
            
        if is_dir:
            if target_name in dirs:
                full_path = os.path.join(root, target_name)
                candidates.append((full_path, os.path.getmtime(full_path)))
        else:
            if target_name in files:
                full_path = os.path.join(root, target_name)
                candidates.append((full_path, os.path.getmtime(full_path)))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[1], reverse=True)
    best_match = candidates[0][0]
    
    return best_match

def query_sqlite(db_name, browser, query):
    original_path = get_target(browser, db_name, is_dir=False)
    if not original_path:
        return []
    
    temp_dir = tempfile.mkdtemp()
    temp_path = os.path.join(temp_dir, db_name)
    shutil.copy2(original_path, temp_path)
    
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
        print(f"[-] SQLite Error reading {db_name}: {e}")
    finally:
        shutil.rmtree(temp_dir)
    return results

def extract_bookmarks(browser):
    path = get_target(browser, "Bookmarks", is_dir=False)
    if not path: return []
    
    try:
        with open(path, "r", encoding="utf-8") as f: 
            data = json.load(f)
    except Exception as e:
        print(f"[-] Error parsing JSON in {path}: {e}")
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

def extract_history(browser):
    q = "SELECT id, url, title, visit_count, last_visit_time FROM urls ORDER BY last_visit_time DESC LIMIT 2000"
    return query_sqlite("History", browser, q)

def extract_downloads(browser):
    q = "SELECT id, target_path, received_bytes, total_bytes, start_time FROM downloads ORDER BY start_time DESC LIMIT 500"
    return query_sqlite("History", browser, q)

def extract_top_sites(browser):
    q = "SELECT url_rank, url, title FROM top_sites ORDER BY url_rank ASC LIMIT 50"
    return query_sqlite("Top Sites", browser, q)

def extract_extensions(browser):
    ext_path = get_target(browser, "Extensions", is_dir=True)
    if not ext_path: return []
    
    results = []
    for ext_id in os.listdir(ext_path):
        id_path = os.path.join(ext_path, ext_id)
        if not os.path.isdir(id_path): continue
        
        versions = os.listdir(id_path)
        if not versions: continue
        
        manifest_path = os.path.join(id_path, versions[0], "manifest.json")
        if os.path.exists(manifest_path):
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest = json.load(f)
                    name = manifest.get("name", "")
                    if name.startswith("__MSG_"): name = ext_id 
                    results.append({"id": ext_id, "name": name, "version": manifest.get("version", "")})
            except:
                pass
    return results

def extract_tabs(browser):
    session_dir = get_target(browser, "Sessions", is_dir=True)
    if not session_dir: return []
    
    urls = set()
    for file in os.listdir(session_dir):
        if file.startswith("Tabs_") or file.startswith("Session_"):
            try:
                with open(os.path.join(session_dir, file), "rb") as f:
                    content = f.read().decode('ascii', errors='ignore')
                    found_urls = re.findall(r'(https?://[^\s\x00]+)', content)
                    urls.update(found_urls)
            except:
                pass
    return [{"url": u} for u in list(urls)]

class BrowserAPIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        path_parts = self.path.strip("/").split("/")
        if len(path_parts) == 2:
            browser, data_type = path_parts
            if browser in ["chrome", "edge", "brave"]:
                funcs = {
                    "bookmarks": extract_bookmarks, "history": extract_history,
                    "downloads": extract_downloads, "top_sites": extract_top_sites,
                    "extensions": extract_extensions, "tabs": extract_tabs
                }
                if data_type in funcs:
                    data = funcs[data_type](browser)
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
    server = HTTPServer(("127.0.0.1", port), BrowserAPIHandler)
    print("Starting robust local browser server on http://127.0.0.1:" + str(port))
    server.serve_forever()