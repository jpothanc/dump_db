import json
import os

from src.common.caches.disk_cache import DiskCache
from src.common.helper import split_into_subsets, zip_directory


class DiskCacheJson(DiskCache):

    def save(self, header, data):
        try:
            local_cache_dir = self.get_local_cache_path()
            os.makedirs(local_cache_dir, exist_ok=True)
            subsets = split_into_subsets(data, self.rows_per_file)
            total = 0

            for i, subset in enumerate(subsets, 1):
                file_name = f"{self.cache_name}.json" if len(subsets) == 1 else f"{self.cache_name}_{i}.json"
                file_path = os.path.join(local_cache_dir, file_name)
                self._save_file(header, subset, file_path)
                total += len(subset)

            self.logger.info(f"total records saved: {total}")
            if self.can_zip:
                zip_directory(local_cache_dir, local_cache_dir)
        except Exception as e:
            self.logger.error(f"error saving cache: {e}")
            return False
        self.logger.debug(f"cache saved to {file_path}")
        return True

    def get_local_cache_path(self):
        return os.path.join(self.cache_dir, self.cache_name) if self.can_zip else self.cache_dir

    def _save_file(self, header, data, file_path):
        try:
            # Log incoming data for debugging
            self.logger.debug(f"Saving file with header: {header}")
            self.logger.debug(f"Data length: {len(data)}")
            
            # Convert data to list of dictionaries using header as keys
            json_data = [dict(zip(header, row)) for row in data]
            self.logger.debug(f"JSON data length: {len(json_data)}")
            
            def decimal_default(obj):
                if hasattr(obj, 'to_eng_string'):
                    return str(obj)
                raise TypeError

            with open(file_path, 'w', encoding='utf-8') as json_file:
                json.dump(json_data, json_file, indent=2, default=decimal_default)
        except Exception as e:
            self.logger.error(f"error saving file: {e}")
            return False
        return True 