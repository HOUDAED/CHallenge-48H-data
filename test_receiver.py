"""
Minimal HTTP server that prints whatever send_data.py POSTs to it.

Usage (terminal 1):  python3 test_receiver.py
Usage (terminal 2):  python3 send_data.py --url http://localhost:9000 --batch 10
"""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        data = json.loads(body)

        print(f"\n--- Received {len(data)} station(s) ---")
        for s in data:
            print(f"  {s['id']:12s}  {s['name']:30s}  ATMO={next((i['value'] for i in s['indices'] if i['id']=='ATMO'), 'n/a')}")

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def log_message(self, *args):
        pass  # silence default access log


if __name__ == "__main__":
    server = HTTPServer(("localhost", 9000), Handler)
    print("Listening on http://localhost:9000 — waiting for POST requests...")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
        server.server_close()
