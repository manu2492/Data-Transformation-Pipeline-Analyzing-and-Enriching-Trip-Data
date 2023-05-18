from decouple import config

USER = config("DB_USER")
PASSWORD = config('DB_PASSWORD')
DB_NAME = config('DB_NAME')