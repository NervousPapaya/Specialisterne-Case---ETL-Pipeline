
from app.load.db.connection import Connector

def main():
    connector = Connector()
    connector.connect()



if __name__ == "__main__":
    main()