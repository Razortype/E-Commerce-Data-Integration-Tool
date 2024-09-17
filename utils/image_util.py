
import requests
from config import ASSET_PATH, IMAGE_PATH, logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

from typing import List, Tuple, Dict
from utils.custom_types import URL

from enum import Enum

class ImageManager:
    
    def __init__(self, 
                 num_workers: int = 5,
                 clear_after_exec: bool = True) -> None:
        
        self.num_workers = num_workers
        self.clear_after_exec = clear_after_exec

        self.img_urls: Dict[str, URL] = {}
        self.not_valid_names: List[str] = []
        self.pool = None

        self.logger = self.logger = logging.getLogger(__name__)

    def fit(self, *image_urls: List[Tuple[str, URL]]):

        for name, url in image_urls:
            self.img_urls[name] = url

    def check(self):
        

        # from collections import Counter

        # counts = Counter(list(self.img_urls.values()))
        # duplicates = [item for item, count in counts.items() if count > 1]

        # for dp in list(set(duplicates)):
        #     print(dp)

        self._load_pool()

        for name, img in self.img_urls.items():
            futures = [self.pool.submit(lambda : self._check_url(name, img))]
        
        print(self.not_valid_names)
        

    def process(self):

        self._load_pool()

        for name, img in self.img_urls.items():
            futures = [self.pool.submit(lambda : self.download_image(IMAGE_PATH, name, img))]

    def clear(self):
        del self.img_urls
        del self.valid_urls
        self.img_urls = {}
        self.not_valid_names = []

    def _check_url(self, name, url):
        is_valid: bool = self.is_image_url_valid(url)
        if not is_valid:
            self.not_valid_names.append(name)

    def _load_pool(self):
        self.pool = ThreadPoolExecutor(max_workers=self.num_workers)

    @staticmethod
    def is_image_url_valid(url):
        try:
            response = requests.head(url)
            if response.status_code == 200:
                content_type = response.headers.get('content-type')
                if 'image' in content_type:
                    print('\033[92mImage URL is valid.\033[0m', ":", url)  # Print in green color
                    return True
                else:
                    print('\033[93mURL does not point to an image.\033[0m', ":", url)  # Print in yellow color
            else:
                print('\033[91mURL is not accessible.\033[0m', ":", url)  # Print in red color
        except requests.RequestException:
            print('\033[91mAn error occurred while checking the URL.\033[0m')  # Print in red color
        return False
    
    def download_image(self, path: str, name: str, url: str):
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            full_path = os.path.join(path, name+".jpg")

            # Open the file in write-binary mode and write the content
            with open(full_path, 'wb') as file:
                for chunk in response.iter_content(1024):
                    file.write(chunk)

            self.logger.info(f"Image successfully downloaded: {full_path}")
        except requests.HTTPError as http_err:
            self.logger.warn(f"HTTP error occurred: {http_err}")
        except Exception as err:
            self.logger.error(f"Other error occurred: {err}")