#!/usr/bin/env python3
import os
import time
import json
import logging
import requests
import zipfile
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from configparser import ConfigParser

# ---------------------------
# Setup logging
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("bam-uploader.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------------------------
# Config loading
# ---------------------------
def load_config(config_path="config.ini"):
    config = ConfigParser()
    config.read(config_path)
    return config

# ---------------------------
# API client
# ---------------------------
class APIClient:
    def __init__(self, base_url, identifier, password, token_file=".token"):
        self.base_url = base_url.rstrip('/')
        self.identifier = identifier
        self.password = password
        self.token = None
        self.token_file = token_file
        self.token = None

        # Try loading a stored token first
        self._load_token()

        # Verify the token; if invalid, reauthenticate
        if not self._is_token_valid():
            logger.info("Token missing or invalid, reauthenticating...")
            self.authenticate()
            self._save_token()
        else:
            logger.info("Valid token found, using existing session.")

    # ---------------------------
    # Token handling
    # ---------------------------
    def _token_path(self):
        return os.path.abspath(self.token_file)

    def _save_token(self):
        with open(self._token_path(), "w") as f:
            f.write(self.token)
        logger.info(f"Token saved to {self._token_path()}")

    def _load_token(self):
        if os.path.exists(self._token_path()):
            with open(self._token_path(), "r") as f:
                self.token = f.read().strip()
            logger.debug("Loaded token from file")

    def _is_token_valid(self):
        """Check if token works by calling /user/me"""
        if not self.token:
            return False
        url = f"{self.base_url}/users/me/"
        try:
            r = requests.get(url, headers=self._headers(), timeout=5)
            if r.status_code == 200:
                return True
            if 400 <= r.status_code < 500:
                logger.warning(f"Token invalid (HTTP {r.status_code})")
                return False
        except requests.RequestException as e:
            logger.error(f"Token validation failed: {e}")
            return False
        return False

    def authenticate(self):
        url = f"{self.base_url}/auth/login/"
        payload = {"identifier": self.identifier, "password": self.password}
        headers = {"Content-Type": "application/json"}
        logger.info(f"Authenticating at {url}")
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        self.token = response.json().get("access")
        if not self.token:
            raise RuntimeError("No token in authentication response")
        logger.info("Authentication successful")

    def _headers(self):
        return {"Authorization": f"Bearer {self.token}"} if self.token else {}

    def create_run(self, run_name):
        url = f"{self.base_url}/runs"
        payload = {"name": run_name}
        response = requests.post(url, json=payload, headers=self._headers())
        response.raise_for_status()
        return response.json()

    def upload_file(self, run_id, file_path):
        url = f"{self.base_url}/runs/{run_id}/attachments"
        files = {"file": open(file_path, "rb")}
        response = requests.post(url, files=files, headers=self._headers())
        response.raise_for_status()
        return response.json()

# ---------------------------
# Watcher logic
# ---------------------------
class FlagFileHandler(FileSystemEventHandler):
    def __init__(self, client, watch_root, flag_suffix=".done", quiescent_check=False, quiet_period=10):
        """
        :param client: APIClient instance
        :param watch_root: Root directory to watch
        :param flag_suffix: Expected suffix for flag files (default: .done)
        :param quiescent_check: If True, will verify no file in dir changed recently before upload
        :param quiet_period: Seconds of inactivity before considering directory stable
        """
        self.client = client
        self.watch_root = Path(watch_root)
        self.flag_suffix = flag_suffix
        self.quiescent_check = quiescent_check
        self.quiet_period = quiet_period

    def on_created(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)

        # Only trigger if it's a flag file (e.g. upload.done)
        if path.name.endswith(self.flag_suffix):
            logger.info(f"Detected flag file: {path}")
            self._handle_flag(path)

    def _handle_flag(self, flag_path):
        """Process a completed run once the flag file appears."""
        # Parent directory of the flag file
        run_dir = flag_path.parent
        run_name = run_dir.name

        logger.info(f"Preparing to process run: {run_name}")

        # Ensure authentication is valid
        if not self.client._is_token_valid():
            logger.warning("Token invalid, reauthenticating before upload...")
            self.client.authenticate()
            self.client._save_token()

        # Optional check: ensure directory isn't still being modified
        if self.quiescent_check:
            logger.info(f"Waiting for directory {run_dir} to be stable...")
            while not self._is_directory_quiescent(run_dir):
                time.sleep(self.quiet_period)

        # Create a new run on the server
        logger.info(f"Creating run in API: {run_name}")
        run = self.client.create_run(run_name)
        run_id = run.get("id")
        logger.info(f"Run created: {run_name} (ID={run_id})")

        # Upload files from subdirectories
        for sub in run_dir.iterdir():
            if sub.is_dir():
                self._upload_subdir(sub, run_id)
            elif sub.is_file() and sub.name != flag_path.name:
                logger.info(f"Uploading root-level file: {sub}")
                self.client.upload_file(run_id, sub)

        logger.info(f"Completed processing of run {run_name}")

    def _upload_subdir(self, subdir, run_id):
        logger.info(f"Uploading files from {subdir}")
        for file in subdir.rglob('*'):
            if file.is_file():
                try:
                    logger.info(f"Uploading file: {file}")
                    self.client.upload_file(run_id, file)
                except Exception as e:
                    logger.error(f"Failed to upload {file}: {e}")

    def _is_directory_quiescent(self, path):
        """Check if no file has been modified recently."""
        latest_mtime = max(f.stat().st_mtime for f in Path(path).rglob('*') if f.is_file())
        age = time.time() - latest_mtime
        return age > self.quiet_period


# ---------------------------
# Entry point
# ---------------------------
def main():
    config = load_config()
    base_url = config["API"]["base_url"]
    identifier = config["API"]["identifier"]
    password = config["API"]["password"]
    watch_path = config["WATCH"]["path"]

    client = APIClient(base_url, identifier, password)

    event_handler = FlagFileHandler(
        client,
        watch_path,
        flag_suffix=".done",         # looks for "upload.done" or similar
        quiescent_check=False,       # you can enable it later if needed
        quiet_period=10
    )

    observer = Observer()
    observer.schedule(event_handler, watch_path, recursive=True)
    observer.start()

    logger.info(f"Watching {watch_path} for FLAG files...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
