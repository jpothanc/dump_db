from src.common.database.db_service import DbService


class DbMssqlService(DbService):
    def __init__(self, connection_string):
        super().__init__(connection_string)

    def connect(self):
        import pyodbc
        return pyodbc.connect(self.connection_string)

    def get_all_tables_query(self):
        return """
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
        """ 