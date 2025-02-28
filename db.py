import contextlib
import psycopg2

@contextlib.contextmanager
def connect():
    conn = None
    try:
        conn = psycopg2.connect('postgresql://postgres:code1819@localhost:5432/measurements')
        yield conn
    finally:
        if conn is not None:
            conn.close()
    