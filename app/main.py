from app.pipeline.etl import ETLProcess
from app.load.db.initialize import DatabaseInitializer
from app.load.db.CRUD import CRUD
from app.config import spec_token, local_database_schema


def main():
    print("hej")
    docker = False
    #initializer = DatabaseInitializer(docker=docker)
    #initializer.create_db()
    #initializer.initialize_db()

    #crud = CRUD()
    #crud.reset_everything(True)

    etl_process = ETLProcess(docker=docker)
    etl_process.update_database()



if __name__ == "__main__":
    main()