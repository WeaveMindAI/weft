"""Exercise the shipped public proxy against a fake dispatcher using Docker."""

import http.client
import http.server
import pathlib
import socket
import subprocess
import tempfile
import threading
import time
import unittest


MANIFEST = pathlib.Path(__file__).resolve().parents[1] / "k8s/public-tunnel.yaml"


class Dispatcher(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        self.server.requests.append((self.command, self.path, body))
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"dispatcher")

    do_GET = do_POST
    do_HEAD = do_POST
    do_OPTIONS = do_POST
    do_PUT = do_POST
    do_PATCH = do_POST
    do_DELETE = do_POST

    def log_message(self, *args):
        pass


class PublicProxyTest(unittest.TestCase):
    def setUp(self):
        self.upstream = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Dispatcher)
        self.upstream.requests = []
        self.addCleanup(self.upstream.server_close)
        thread = threading.Thread(target=self.upstream.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(self.upstream.shutdown)

        manifest = MANIFEST.read_text()
        block = manifest.split("  default.conf: |\n", 1)[1].split("\n---", 1)[0]
        config = "\n".join(line[4:] for line in block.splitlines())
        image = manifest.split("image: nginx:", 1)[1].splitlines()[0].strip()
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            self.port = reservation.getsockname()[1]
        config = config.replace("listen 8080;", f"listen 127.0.0.1:{self.port};")
        config = config.replace("${CLUSTER_DNS}", "127.0.0.1")
        config = config.replace(
            "http://weft-dispatcher.weft-system.svc.cluster.local:9999",
            f"http://127.0.0.1:{self.upstream.server_port}",
        )
        directory = tempfile.TemporaryDirectory(prefix="weft-public-proxy-")
        self.addCleanup(directory.cleanup)
        path = pathlib.Path(directory.name) / "nginx.conf"
        path.write_text("events {}\nhttp {\n" + config + "\n}\n")
        container = subprocess.check_output([
            "docker", "run", "--detach", "--network", "host",
            "--mount", f"type=bind,src={path},dst=/test.conf,readonly",
            "--entrypoint", "nginx", f"nginx:{image}",
            "-c", "/test.conf", "-g", "daemon off;",
        ], text=True).strip()
        self.addCleanup(subprocess.run, ["docker", "rm", "--force", container],
                        check=True, stdout=subprocess.DEVNULL)
        deadline = time.monotonic() + 10
        while True:
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.1):
                    break
            except OSError:
                running = subprocess.check_output(
                    ["docker", "inspect", "--format", "{{.State.Running}}", container],
                    text=True,
                ).strip()
                if running != "true" or time.monotonic() >= deadline:
                    logs = subprocess.check_output(["docker", "logs", container],
                                                   stderr=subprocess.STDOUT, text=True)
                    self.fail(f"Public proxy did not start: {logs}")
                time.sleep(0.05)

    def request(self, method, path, body=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
        try:
            connection.request(method, path, body=body)
            response = connection.getresponse()
            response.read()
            return response.status
        finally:
            connection.close()

    def test_provider_posts_pass_but_monitoring_never_reaches_dispatcher(self):
        for path in ("/events/slack/messages", "/events/google/mail"):
            with self.subTest(path=path):
                self.assertEqual(self.request("POST", path, b"event payload"), 200)
                self.assertEqual(self.upstream.requests[-1], ("POST", path, b"event payload"))

        count = len(self.upstream.requests)
        for method in ("GET", "HEAD", "OPTIONS", "PUT", "PATCH", "DELETE"):
            for path in ("/events/project/example", "/events/execution/example",
                         "/events/slack/messages"):
                with self.subTest(method=method, path=path):
                    self.assertEqual(self.request(method, path), 403)
        self.assertEqual(len(self.upstream.requests), count)

        self.assertEqual(self.request("POST", "/signal/example", b"answer"), 200)
        self.assertEqual(self.request("GET", "/signal-token/signals"), 200)
        self.assertEqual(self.request("GET", "/public/files/example"), 200)
        self.assertEqual(self.request("GET", "/projects"), 404)


if __name__ == "__main__":
    unittest.main()
