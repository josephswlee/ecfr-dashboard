import sqlite3
import pandas as pd

def get_db_connection(db_path='data/cfr.db'):
    """
    Establishes a connection to the SQLite database
    """
    conn = sqlite3.connect(db_path)
    return conn

def get_agencies_data(db_path='data/cfr.db'):
    """
    Fetches all data from the agencies table
    """
    conn = get_db_connection(db_path)
    df = pd.read_sql_query("SELECT * FROM agencies", conn)
    conn.close()
    return df

def get_cfr_sections_data(db_path='data/cfr.db'):
    """
    Fetches all data from the cfr_sections table
    """
    conn = get_db_connection(db_path)
    df = pd.read_sql_query("SELECT * FROM cfr_sections", conn)
    conn.close()
    return df

def get_cfr_references_data(db_path='data/cfr.db'):
    """
    Fetches all data from the cfr_references table
    """
    conn = get_db_connection(db_path)
    df = pd.read_sql_query("SELECT * FROM cfr_references", conn)
    conn.close()
    return df
