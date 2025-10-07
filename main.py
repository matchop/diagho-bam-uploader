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
        print(f"token path: {self.token_file}")
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
    def __init__(self, client, watch_root):
        self.client = client
        self.watch_root = Path(watch_root)

    def on_created(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith("FLAG"):
            flag_path = Path(event.src_path)
            subdir = flag_path.parent
            logger.info(f"Detected FLAG in {subdir}")
            self.process_directory(subdir)

    def process_directory(self, subdir):
        run_name = subdir.name
        run = self.client.create_run(run_name)
        run_id = run.get("id")
        logger.info(f"Created run {run_name} (ID {run_id})")

        for file in subdir.iterdir():
            if file.is_file() and not file.name.endswith("FLAG"):
                logger.info(f"Uploading file: {file}")
                self.client.upload_file(run_id, file)

        # Example of processing a zipped file
        zip_file = subdir / "special_content.zip"
        if zip_file.exists():
            extract_dir = subdir / "extracted"
            extract_dir.mkdir(exist_ok=True)
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            for f in extract_dir.iterdir():
                logger.info(f"Uploading extracted file: {f}")
                self.client.upload_file(run_id, f)

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
    # client.authenticate()

    # event_handler = FlagFileHandler(client, watch_path)
    # observer = Observer()
    # observer.schedule(event_handler, watch_path, recursive=True)
    # observer.start()

    logger.info(f"Watching {watch_path} for FLAG files...")

    # try:
    #     while True:
    #         time.sleep(1)
    # except KeyboardInterrupt:
    #     observer.stop()
    # observer.join()

if __name__ == "__main__":
    main()
