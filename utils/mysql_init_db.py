from mysql import connector

class MysqlInit(object):
    def __init__(self, host, user, password, database) -> None:
        self.host = host
        self.user = user
        self.password = password
        self.database_name = database
        self.init_db()
        
    def init_db(self):
        mydb = connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database = self.database_name
        )
        return mydb
    
    def get_cursor(self,mydb):
        try:
            mydb.ping(reconnect=True, attempts=3, delay=5)
        except connector.Error as err:
            mydb = self.init_db()
        return mydb.cursor(buffered=True)