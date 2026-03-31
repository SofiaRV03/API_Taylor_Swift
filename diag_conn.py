import os
import sys
from dotenv import load_dotenv

# Intentar cargar variables de entorno
load_dotenv()

def diagnostic():
    print(f"Python Version: {sys.version}")
    print(f"Platform: {sys.platform}")
    
    vars_to_check = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    
    print("\n--- Environment Variables (from .env) ---")
    for v in vars_to_check:
        val = os.getenv(v)
        if val:
            print(f"{v}: {val} (Length: {len(val)})")
            # Verificar si hay caracteres no-ascii
            try:
                val.encode('ascii')
            except UnicodeEncodeError:
                print(f"  Warning: {v} contains non-ASCII characters!")
        else:
            print(f"{v}: NOT SET")

    # Reconstruir la URL de conexión (con password oculto)
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'taylor_swift')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '******')
    
    url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    print(f"\nConstructed URL: {url}")
    print(f"URL Length: {len(url)}")
    
    # Probar conexión básica con psycopg2
    try:
        import psycopg2
        print("\n--- Testing psycopg2 directly ---")
        conn_str = f"host={os.getenv('DB_HOST')} port={os.getenv('DB_PORT')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}"
        print(f"Attempting connect with DSN: host={os.getenv('DB_HOST')} ...")
        conn = psycopg2.connect(conn_str)
        print("Connection successful!")
        conn.close()
    except Exception as e:
        print(f"Error testing psycopg2: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    diagnostic()
