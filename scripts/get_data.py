import argparse
import json
import logging
import os
import sys
import unicodedata
import re
import time
from typing import List
import hashlib
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

config_template = {
    "dataverse_url": "https://darus.uni-stuttgart.de/",
    "datasets": [
        {"id": "doi:10.18419/darus-????", "version": ":latest"}
    ],
}

MAX_WORKERS = 4

def get_script_path() -> str:
    return os.path.dirname(os.path.realpath(sys.argv[0]))

def get_search_dirs() -> List[str]:
    return [
        get_script_path(),
        os.path.join(get_script_path(), ".."),
        os.path.join(get_script_path(), "../.."),
    ]

def slugify(value, allow_unicode=False):
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize("NFKC", value)
    else:
        value = (
            unicodedata.normalize("NFKD", value)
            .encode("ascii", "ignore")
            .decode("ascii")
        )
    value = re.sub(r"[^\w\s-]", "", value.lower())
    return re.sub(r"[-\s]+", "-", value).strip("-_")

def create_config_template_if_needed() -> bool:
    if not any([os.path.exists(os.path.join(d, "darus_config.json")) for d in get_search_dirs()]):
        print("No configuration file exists.")
        with open(os.path.join(get_script_path(), "darus_config.json"), "w") as cf:
            json.dump(config_template, cf, indent=4)
        print("Created a config template.\nRemember to fill in your dataset identifiers and create a .darus_apikey file.")
        return True
    return False

def load_api_key_from_file():
    for d in get_search_dirs():
        path = os.path.join(d, ".darus_apikey")
        if os.path.exists(path):
            with open(path, "r") as key_file:
                return key_file.read().strip()
    print("No file .darus_apikey found. Proceeding with public authentication.")
    return None

def load_config_from_file():
    config_txt = ""
    for d in get_search_dirs():
        path = os.path.join(d, "darus_config.json")
        if os.path.exists(path):
            with open(path, "r") as config_file:
                config_txt = config_file.read()
            break
    return json.loads(config_txt)

def get_headers(api_token: str | None = None):
    headers = {}
    if api_token and api_token != "":
        headers["X-Dataverse-key"] = api_token
    return headers

def get_dataset_info(session: requests.Session, dataset_obj: dict, config: dict, api_token: str | None = None):
    dataset_id = dataset_obj["id"]
    dataset_version = dataset_obj["version"]
    if dataset_version in ["latest", "latest-published", "draft"]:
        dataset_version = ":" + dataset_version
    headers = get_headers(api_token)
    url = f"{config['dataverse_url']}api/v1/datasets/:persistentId/versions/{dataset_version}/?persistentId=doi:{dataset_id.replace('doi:', '')}"
    return session.get(url, headers=headers)

def calculate_md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(100 * 4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


class FileCounter:
    def __init__(self, total_files):
        self.completed = 0
        self.total = total_files
        self.lock = Lock()

    def increment(self):
        with self.lock:
            self.completed += 1
            return self.completed


def download_single_file(session: requests.Session, file_info: dict, folder: str, config_obj: dict, api_key: str, global_pbar: tqdm, file_counter: FileCounter):
    file_name = file_info["dataFile"]["filename"]
    file_id = file_info["dataFile"]["id"]
    file_md5 = file_info["dataFile"]["md5"]
    file_size = int(file_info["dataFile"]["filesize"])
    subfolder = file_info.get("directoryLabel", "")

    fullpath = os.path.join(folder, subfolder, file_name)

    if os.path.exists(fullpath) and os.path.isfile(fullpath):
        if calculate_md5(fullpath) == file_md5:
            logging.info(f"File `{file_name}` is up to date according to md5.")
            global_pbar.update(file_size)
            current_count = file_counter.increment()
            global_pbar.set_postfix_str(f"Files: {current_count}/{file_counter.total}")
            return

    headers = get_headers(api_key)
    file_url = f"{config_obj['dataverse_url']}api/v1/access/datafile/{file_id}"
    
    try:
        with session.get(file_url, headers=headers, stream=True, timeout=30) as file_resp:
            if file_resp.ok:
                os.makedirs(os.path.dirname(fullpath), exist_ok=True)
                chunk_size = 2 * 1024 * 1024

                with open(fullpath, "wb") as f:
                    for chunk in file_resp.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            global_pbar.update(len(chunk))
            else:
                logging.warning(f"Failed to access file {file_name}: {file_resp.text}")
                global_pbar.update(file_size)
    except Exception as e:
        logging.error(f"Error downloading {file_name}: {e}")
        global_pbar.update(file_size)
    finally:
        current_count = file_counter.increment()
        global_pbar.set_postfix_str(f"Files: {current_count}/{file_counter.total}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="get_data", description="Downloads data from configured DaRUS repositories.")
    parser.add_argument("-log", "--log", default="warning", help="Provide logging level.")
    options = parser.parse_args()

    levels = {'critical': logging.CRITICAL, 'error': logging.ERROR, 'warning': logging.WARNING, 'info': logging.INFO, 'debug': logging.DEBUG}
    logging.basicConfig(level=levels.get(options.log.lower(), logging.WARNING))

    if create_config_template_if_needed():
        sys.exit(0)

    config_obj = load_config_from_file()
    api_key = load_api_key_from_file()

    start_time = time.perf_counter()

    with requests.Session() as http_session:
        adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS * 2)
        http_session.mount('https://', adapter)
        http_session.mount('http://', adapter)

        for dataset_identifier in config_obj["datasets"]:
            dataset_resp = get_dataset_info(http_session, dataset_identifier, config_obj, api_key)

            if dataset_resp.ok:
                dataset_data = dataset_resp.json()["data"]
                citation_info = dataset_data["metadataBlocks"]["citation"]["fields"]
                dataset_title = next((c["value"] for c in citation_info if c["typeName"] == "title"), "dataset")

                folder_name = slugify(dataset_title)
                base_data_dir = os.path.join(get_script_path(), "data")
                folder = os.path.normpath(os.path.join(base_data_dir, folder_name))
                
                if os.path.exists(folder):
                    download_data = input(f"The folder {folder} already exists. Do you want to download again? (y/n) ")
                    if download_data.lower() == "n":
                        continue
                os.makedirs(folder, exist_ok=True)

                all_files = dataset_data["files"]
                total_files_count = len(all_files)
                total_dataset_bytes = sum(int(file["dataFile"]["filesize"]) for file in all_files)
                
                total_gb_est = total_dataset_bytes / (1024**3)
                print(f"\nInitializing parallel download pipeline for: {dataset_title}")
                print(f"Total Stats: {total_gb_est:.2f} GB divided across {total_files_count} files.")
                print(f"Destination: {folder}")

                file_counter = FileCounter(total_files_count)

                with tqdm(
                    total=total_dataset_bytes,
                    unit='B',
                    unit_scale=True,
                    unit_divisor=1024,
                    desc="TOTAL DATASET PROGRESS",
                    bar_format="{desc}: {percentage:3.1f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}{postfix}]"
                ) as global_pbar:
                    global_pbar.set_postfix_str(f"Files: 0/{total_files_count}")

                    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        futures = [
                            executor.submit(download_single_file, http_session, file, folder, config_obj, api_key, global_pbar, file_counter)
                            for file in all_files
                        ]
                        for future in as_completed(futures):
                            pass

                with open(os.path.join(folder, "info.json"), "w") as json_metadata_f:
                    json.dump(dataset_data, json_metadata_f, indent=4)
            else:
                logging.error(f"Failed to download dataset: {dataset_resp.text}")

    end_time = time.perf_counter()
    elapsed_total = end_time - start_time

    hours, remainder = divmod(int(elapsed_total), 3600)
    minutes, seconds = divmod(remainder, 60)

    print("\n" + "="*50)
    print(f"Download complete! Total execution time: {hours}h {minutes}m {seconds}s")
    print("="*50)