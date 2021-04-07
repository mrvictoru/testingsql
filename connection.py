import config
import cx_Oracle

def connect():
    # connect to Oracle DB
    connection = None
    try:
        connection = cx_Oracle.connect(
            config.username,
            config.password,
            config.dsn,
            encoding = "UTF-8"
        )
        print("Connected")
        print(connection.version)
    except cx_Oracle.Error as error:
        print(error)
    
    return connection