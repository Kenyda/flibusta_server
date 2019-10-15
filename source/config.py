import fire


class Config:
    DEBUG = False

    SERVER_HOST: str
    SERVER_PORT: int

    DB_NAME: str
    DB_HOST: str
    DB_USER: str
    DB_PASSWORD: str

    TEMP_DB_NAME: str
    TEMP_DB_HOST: str
    TEMP_DB_USER: str
    TEMP_DB_PASSWORD: str

    DSN: str

    TOR_PROXIES: str

    def __init__(self, db_password: str, temp_db_password: str,
                 db_name: str = "flibusta", db_host: str = "localhost",
                 db_user: str = "flibusta",
                 temp_db_name = "temp", temp_db_host = "localhost",
                 temp_db_user = "root",
                 server_host: str = "0.0.0.0", server_port: int = 7770,
                 tor_proxies: str = "http://localhost:8118"):
        Config.SERVER_HOST = server_host
        Config.SERVER_PORT = server_port

        Config.DB_NAME = db_name
        Config.DB_HOST = db_host
        Config.DB_USER = db_user
        Config.DB_PASSWORD = db_password

        Config.TEMP_DB_NAME = temp_db_name
        Config.TEMP_DB_HOST = temp_db_host
        Config.TEMP_DB_USER = temp_db_user
        Config.TEMP_DB_PASSWORD = str(temp_db_password)

        Config.DSN = f"postgresql://{self.DB_HOST}:5432/{self.DB_USER}"

        Config.TOR_PROXIES = tor_proxies


fire.Fire(Config)
