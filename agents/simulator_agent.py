"""@simulator agent — Manages phone telemetry simulator and web server."""

import http.server
import logging
import os
import subprocess
import sys
import threading

logger = logging.getLogger(__name__)


class SimulatorAgent:
    """Manages the phone telemetry simulator and web server.

    Supports two modes:
    - Python simulator (phone_simulator.py): 5 virtual devices via Event Hub SDK
    - Web page (phone-telemetry.html): Real device telemetry via browser
    """

    def __init__(self, config: dict, project_dir: str = None):
        self.cfg = config.get("simulator", {})
        self.project_dir = project_dir or os.path.dirname(os.path.dirname(__file__))
        self._processes = {}

    def start_python_simulator(self, connection_string: str) -> dict:
        """Start the Python phone simulator (phone_simulator.py)."""
        script = os.path.join(self.project_dir, "phone_simulator.py")
        if not os.path.exists(script):
            logger.error("phone_simulator.py not found at %s", script)
            return {"status": "error", "message": "phone_simulator.py not found"}

        env = os.environ.copy()
        env["EVENT_HUB_CONNECTION_STRING"] = connection_string
        env["EVENT_HUB_NAME"] = "phone-telemetry"

        proc = subprocess.Popen(
            [sys.executable, script],
            env=env,
            cwd=self.project_dir,
        )
        self._processes["simulator"] = proc
        logger.info("Phone simulator started (PID=%d)", proc.pid)
        return {"status": "running", "pid": proc.pid}

    def start_web_server(self, port: int = 8080) -> dict:
        """Start HTTP server for phone-telemetry.html."""
        html_file = os.path.join(self.project_dir, "phone-telemetry.html")
        if not os.path.exists(html_file):
            logger.error("phone-telemetry.html not found")
            return {"status": "error", "message": "phone-telemetry.html not found"}

        handler = http.server.SimpleHTTPRequestHandler
        server = http.server.HTTPServer(("0.0.0.0", port), handler)

        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self._processes["web_server"] = server

        logger.info("Web server started on http://0.0.0.0:%d", port)
        logger.info("Open phone-telemetry.html from your phone's browser")
        return {"status": "running", "port": port}

    def stop(self):
        """Stop all simulator processes."""
        if "simulator" in self._processes:
            self._processes["simulator"].terminate()
            logger.info("Phone simulator stopped")
        if "web_server" in self._processes:
            self._processes["web_server"].shutdown()
            logger.info("Web server stopped")
        self._processes.clear()

    def status(self) -> dict:
        """Check status of simulator components."""
        result = {}
        if "simulator" in self._processes:
            proc = self._processes["simulator"]
            result["simulator"] = {
                "running": proc.poll() is None,
                "pid": proc.pid,
            }
        if "web_server" in self._processes:
            result["web_server"] = {"running": True}
        return result if result else {"status": "not running"}

    def deploy(self) -> dict:
        """Start the web server for phone telemetry capture."""
        logger.info("=== @simulator: Starting phone telemetry server ===")
        result = self.start_web_server()
        logger.info(
            "Phone telemetry page available. "
            "Open phone-telemetry.html from any device on the network."
        )
        logger.info("=== @simulator: Ready ===")
        return result
