from app.pipeline.etl import ETLProcess
from app.load.db.initialize import DatabaseInitializer
from app.config import docker
import os
from app.load.db.CRUD import CRUD

def main():
    initializer = DatabaseInitializer(docker=docker)
    initializer.create_db()
    initializer.initialize_db()

    #crud = CRUD()
    #crud.reset_everything()

    etl_process = ETLProcess(docker=docker)

    if docker:
        mode = os.getenv("ETL_MODE", "once")
        interval = int(os.getenv("ETL_INTERVAL", 10))

        if mode == "once":
            etl_process.update_database()
        elif mode == "interval":
            etl_process.docker_etl_background(interval_minutes=interval)
    else:
        etl_process.user_controlled_update()


if __name__ == "__main__":
    main()