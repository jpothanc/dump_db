import logging

from src.common.caches.disk_cache_type import DiskFileType
from src.common.database.db_type import DbType
from src.dbdisk.db_disk_cache_builder import DbDiskCacheBuilder

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    CONNECTION_STRING = "postgresql://jpothanc:Z2UXaMsCO3HV@ep-white-forest-89963536.ap-southeast-1.aws.neon.tech/datastore"
    try:
        result = DbDiskCacheBuilder.create(lambda x: (
            x.set_db_type(DbType.POSTGRESQL)
            .set_connection_string(CONNECTION_STRING)
            .set_disk_file_type(DiskFileType.CSV)
            .set_cache_path(r"C:\temp\diskCache")
            .set_cache_name("users")
            .set_query("select * from equities")
            .set_rows_per_file(1000)
            .set_can_zip(True)
            .set_output_file("db_cache_results.txt")
        )).execute()
        print(result.to_json())
    except Exception as e:
        print(e)
    finally:
        print("Completed")