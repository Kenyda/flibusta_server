import fire


class Config:
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

    @classmethod
    def configure(cls, db_password: str, temp_db_password: str,
                  db_name: str = "flibusta", db_host: str = "localhost",
                  db_user: str = "flibusta",
                  temp_db_name = "temp", temp_db_host = "localhost",
                  temp_db_user = "root",
                  server_host: str = "localhost", server_port: int = 7770,
                  tor_proxies: str = "http://localhost:8118"):
        cls.SERVER_HOST = server_host
        cls.SERVER_PORT = server_port

        cls.DB_NAME = db_name
        cls.DB_HOST = db_host
        cls.DB_USER = db_user
        cls.DB_PASSWORD = db_password

        cls.TEMP_DB_NAME = temp_db_name
        cls.TEMP_DB_HOST = temp_db_host
        cls.TEMP_DB_USER = temp_db_user
        cls.TEMP_DB_PASSWORD = str(temp_db_password)

        cls.DSN = f"postgresql://{cls.DB_HOST}:5432/{cls.DB_USER}"

        cls.TOR_PROXIES = tor_proxies


fire.Fire(Config.configure)
