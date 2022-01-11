import cx_Oracle

class connection():
    # connect to Oracle DB
    def __init__(self,config):
        self.username = config.username
        self.password = config.password
        self.dsn = config.dsn
        self.port = config.port
        self.connecting = None
        self.cursor = None
    
    def connect(self):
        try:
            self.connecting = cx_Oracle.connect(
                self.username,
                self.password,
                self.dsn,
                encoding = "UTF-8"
            )
            print("Connected")
            print(self.connecting.version)
            self.cursor = self.connecting.cursor()
        except cx_Oracle.Error as error:
            print(error)


    def close(self):
        if self.connecting:
            self.connecting.close()
            self.connecting = None
            self.cursor = None
            print("Connection closed")
        else:
            print("No connection to close")

    