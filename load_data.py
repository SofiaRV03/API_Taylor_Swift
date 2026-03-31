import pandas as pd
import os
import psycopg
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey, Float
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import text
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración de la base de datos
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'taylor_swift')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '132456')

def create_db_if_not_exists():
    # Conectar a la base de datos por defecto 'postgres' para crear la nueva
    conn = psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname='postgres',
        autocommit=True
    )
    with conn.cursor() as cur:
        # Verificar si la base de datos existe
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
        exists = cur.fetchone()
        if not exists:
            print(f"Creando la base de datos {DB_NAME}...")
            cur.execute(f"CREATE DATABASE {DB_NAME}")
        else:
            print(f"La base de datos {DB_NAME} ya existe.")
    conn.close()

# Ejecutar creación de DB antes de SQLAlchemy
try:
    create_db_if_not_exists()
except Exception as e:
    print(f"Nota: No se pudo verificar/crear la DB automáticamente: {e}")

# URL de conexión para SQLAlchemy
# Usamos psycopg (v3) que es más moderno y maneja mejor las codificaciones en Windows
DATABASE_URL = f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Crear el motor de la base de datos
engine = create_engine(DATABASE_URL)
Base = declarative_base()

# Definir los modelos de las tablas
class Album(Base):
    __tablename__ = 'album'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)
    release_date = Column(String(50))

class Song(Base):
    __tablename__ = 'songs'
    id = Column(Integer, primary_key=True, autoincrement=True)
    album_id = Column(Integer, ForeignKey('album.id'), nullable=False)
    name = Column(String(255), nullable=False)
    spotify_id = Column(String(100))
    uri = Column(String(255))
    track_number = Column(Integer)
    duration_ms = Column(Integer)
    popularity = Column(Integer)
    acousticness = Column(Float)
    danceability = Column(Float)
    energy = Column(Float)
    instrumentalness = Column(Float)
    liveness = Column(Float)
    loudness = Column(Float)
    speechiness = Column(Float)
    tempo = Column(Float)
    valence = Column(Float)

def main():
    # Crear las tablas en la base de datos si no existen
    Base.metadata.create_all(engine)
    print("Tablas verificadas/creadas exitosamente.")

    # Leer el archivo Excel
    excel_file = 'taylor.xlsx'
    df = pd.read_excel(excel_file, sheet_name='taylor_swift_spotify')
    print(f"Archivo {excel_file} cargado exitosamente.")

    # Preparar la sesión
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Cargar álbumes únicos
        albums_unique = df[['album', 'release_date']].drop_duplicates('album')
        
        album_map = {}
        for _, row in albums_unique.iterrows():
            album_name = row['album']
            release_date = str(row['release_date'])
            
            # Verificar si el álbum ya existe
            album = session.query(Album).filter_by(name=album_name).first()
            if not album:
                album = Album(name=album_name, release_date=release_date)
                session.add(album)
                session.flush() # Para obtener el ID del álbum recién creado
            
            album_map[album_name] = album.id

        print(f"Cargados/Verificados {len(albums_unique)} álbumes.")

        # Cargar canciones
        songs_count = 0
        for _, row in df.iterrows():
            # Verificar si la canción ya existe en ese álbum
            song_name = row['name']
            album_id = album_map[row['album']]
            
            existing_song = session.query(Song).filter_by(name=song_name, album_id=album_id).first()
            if not existing_song:
                song = Song(
                    album_id=album_id,
                    name=song_name,
                    spotify_id=row['id'],
                    uri=row['uri'],
                    track_number=row['track_number'],
                    duration_ms=row['duration_ms'],
                    popularity=row['popularity'],
                    acousticness=row['acousticness'],
                    danceability=row['danceability'],
                    energy=row['energy'],
                    instrumentalness=row['instrumentalness'],
                    liveness=row['liveness'],
                    loudness=row['loudness'],
                    speechiness=row['speechiness'],
                    tempo=row['tempo'],
                    valence=row['valence']
                )
                session.add(song)
                songs_count += 1

        session.commit()
        print(f"Cargadas {songs_count} canciones exitosamente.")

    except Exception as e:
        session.rollback()
        print(f"Error al cargar los datos: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    main()
