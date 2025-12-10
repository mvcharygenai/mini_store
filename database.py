# database.py
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, ForeignKey
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship, scoped_session
import urllib.parse
import os
import streamlit as st

Base = declarative_base()

class Customer(Base):
    __tablename__ = "customers"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    email = Column(String, nullable=True)

class Product(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    price = Column(Float, nullable=False, default=0.0)

class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(Integer, ForeignKey("customers.id"))
    product_id = Column(Integer, ForeignKey("products.id"))
    quantity = Column(Integer, nullable=False, default=1)

    customer = relationship("Customer", lazy="joined")
    product = relationship("Product", lazy="joined")

def get_engine():
    """
    Returns an SQLAlchemy engine.
    Looks for credentials in streamlit secrets (preferred) or environment variables.
    """
    # Try Streamlit secrets first (works on Streamlit Cloud)
    secrets = {}
    try:
        secrets = st.secrets["sqlserver"]
    except Exception:
        # not available; fallback to environment variables
        secrets = {
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "server": os.getenv("DB_SERVER"),
            "database": os.getenv("DB_NAME"),
            "driver": os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")
        }

    user = secrets.get("user")
    password = secrets.get("password")
    server = secrets.get("server")  # e.g. myserver.database.windows.net or ip
    database = secrets.get("database")
    driver = secrets.get("driver", "ODBC Driver 18 for SQL Server")

    if not all([user, password, server, database]):
        # fallback to local SQLite (helpful for local dev if creds missing)
        sqlite_url = "sqlite:///store.db"
        return create_engine(sqlite_url, connect_args={"check_same_thread": False})

    # Build ODBC-style connection string and URL-encode it
    params = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )
    odbc_str = urllib.parse.quote_plus(params)
    engine_url = f"mssql+pyodbc:///?odbc_connect={odbc_str}"
    engine = create_engine(engine_url, pool_pre_ping=True)
    return engine

_engine = get_engine()
SessionLocal = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=_engine))

def init_db():
    Base.metadata.create_all(bind=_engine)
