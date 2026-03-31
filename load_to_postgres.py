"""
Carga taylor.xlsx en PostgreSQL.

Uso:
    1. Copia .env.example a .env y llena tus credenciales
    2. pip install pandas openpyxl psycopg2-binary python-dotenv
    3. python load_to_postgres.py
"""

import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

EXCEL_PATH = "taylor.xlsx"
SHEET_NAME = "taylor_swift_spotify"


# ── Conexión ────────────────────────────────────────────────────────────────

def get_connection():
    url = os.getenv("DATABASE_URL")
    if url:
        return psycopg2.connect(url)

    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "taylor_swift"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
    )


# ── Schema ───────────────────────────────────────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS albums (
    id           SERIAL PRIMARY KEY,
    name         TEXT NOT NULL UNIQUE,
    release_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS songs (
    id               TEXT PRIMARY KEY,
    name             TEXT        NOT NULL,
    album_id         INTEGER     NOT NULL REFERENCES albums(id) ON DELETE CASCADE,
    release_date     DATE        NOT NULL,
    track_number     INTEGER,
    uri              TEXT,
    acousticness     NUMERIC(6,4),
    danceability     NUMERIC(6,4),
    energy           NUMERIC(6,4),
    instrumentalness NUMERIC(8,6),
    liveness         NUMERIC(6,4),
    loudness         NUMERIC(8,4),
    speechiness      NUMERIC(6,4),
    tempo            NUMERIC(8,4),
    valence          NUMERIC(6,4),
    popularity       SMALLINT,
    duration_ms      INTEGER
);

CREATE INDEX IF NOT EXISTS idx_songs_album      ON songs(album_id);
CREATE INDEX IF NOT EXISTS idx_songs_popularity ON songs(popularity DESC);
CREATE INDEX IF NOT EXISTS idx_songs_name       ON songs USING gin(to_tsvector('english', name));
"""


# ── ETL ───────────────────────────────────────────────────────────────────────

def load_excel(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=SHEET_NAME)
    df = df.drop(columns=["Column1"], errors="ignore")
    df["release_date"] = pd.to_datetime(df["release_date"]).dt.date

    # Normalizar features de audio a rango 0-1 (vienen escaladas x1000 en el Excel)
    audio_cols = ["acousticness", "danceability", "energy",
                  "liveness", "speechiness", "valence"]
    for col in audio_cols:
        if df[col].max() > 1:
            df[col] = (df[col] / 1000).round(4)

    # loudness y tempo no se normalizan (son dB y BPM)
    df["loudness"] = (df["loudness"] / 1000).round(4)  # viene en mdB
    df["tempo"]    = (df["tempo"]    / 1000).round(4)  # viene en mBPM

    return df


def insert_albums(cur, df: pd.DataFrame) -> dict[str, int]:
    albums = (
        df[["album", "release_date"]]
        .drop_duplicates(subset="album")
        .sort_values("release_date")
    )

    execute_values(cur, """
        INSERT INTO albums (name, release_date)
        VALUES %s
        ON CONFLICT (name) DO UPDATE SET release_date = EXCLUDED.release_date
        RETURNING id, name
    """, [(row.album, row.release_date) for row in albums.itertuples()])

    rows = cur.fetchall()
    return {name: aid for aid, name in rows}


def insert_songs(cur, df: pd.DataFrame, album_ids: dict[str, int]):
    records = [
        (
            row.id, row.name, album_ids[row.album], row.release_date,
            row.track_number, row.uri,
            row.acousticness, row.danceability, row.energy,
            row.instrumentalness, row.liveness, row.loudness,
            row.speechiness, row.tempo, row.valence,
            row.popularity, row.duration_ms,
        )
        for row in df.itertuples()
    ]

    execute_values(cur, """
        INSERT INTO songs (
            id, name, album_id, release_date, track_number, uri,
            acousticness, danceability, energy, instrumentalness,
            liveness, loudness, speechiness, tempo, valence,
            popularity, duration_ms
        ) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            popularity   = EXCLUDED.popularity,
            danceability = EXCLUDED.danceability,
            energy       = EXCLUDED.energy
    """, records)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not Path(EXCEL_PATH).exists():
        print(f"❌  No se encontró {EXCEL_PATH}")
        sys.exit(1)

    print(f"📂  Leyendo {EXCEL_PATH}...")
    df = load_excel(EXCEL_PATH)
    print(f"    {len(df)} canciones | {df['album'].nunique()} álbumes")

    print("🔌  Conectando a PostgreSQL...")
    try:
        conn = get_connection()
    except psycopg2.OperationalError as e:
        print(f"❌  No se pudo conectar:\n    {e}")
        sys.exit(1)

    with conn:
        with conn.cursor() as cur:
            print("🔨  Creando tablas e índices...")
            cur.execute(DDL)

            print("💿  Insertando álbumes...")
            album_ids = insert_albums(cur, df)
            print(f"    {len(album_ids)} álbumes insertados")

            print("🎵  Insertando canciones...")
            insert_songs(cur, df, album_ids)
            print(f"    {len(df)} canciones insertadas")

    conn.close()

    print("\n✅  ¡Listo! Datos cargados en PostgreSQL.")
    print("\n📋  Prueba estas consultas en tu cliente SQL:")
    print("""
    -- Top 5 más populares
    SELECT s.name, a.name AS album, s.popularity
    FROM songs s JOIN albums a ON s.album_id = a.id
    ORDER BY s.popularity DESC LIMIT 5;

    -- Promedio de danceability por álbum
    SELECT a.name, ROUND(AVG(s.danceability)::numeric, 3) AS avg_dance
    FROM songs s JOIN albums a ON s.album_id = a.id
    GROUP BY a.name ORDER BY avg_dance DESC;

    -- Buscar canción por nombre (full-text search)
    SELECT name, popularity FROM songs
    WHERE to_tsvector('english', name) @@ plainto_tsquery('english', 'love');
    """)


if __name__ == "__main__":
    main()
