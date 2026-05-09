# src/whatstheodds/db/init_db.py
import os

from sqlalchemy import Table, create_engine, text
from sqlalchemy.exc import ProgrammingError

from whatstheodds.db.models import Base

# Based on your docker-compose.yml
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASS", "supersecretpassword")
DB_HOST = os.getenv("DB_HOST", "192.168.1.88")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "sofascrape_db")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def initialise_database():
    print(f"Connecting to database @ {DB_HOST}:{DB_PORT}...")
    engine = create_engine(DATABASE_URL)

    with engine.connect() as conn:
        print("Checking/ creating 'betfair' schema .. ")
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS betfair;"))
        conn.commit()

    print("Reflecting existing public tables...")
    Table("events", Base.metadata, autoload_with=engine, schema="public")

    print("Creating tbales in the 'betfair' schema ...")
    Base.metadata.create_all(engine)

    print("Database initialise successful!")


if __name__ == "__main__":
    initialise_database()
