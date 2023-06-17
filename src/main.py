import re
import time
import concurrent.futures
import mysql.connector
from neo4j import GraphDatabase


# Configura a conexão com o banco de dados MySQL
def connect_to_mysql():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="musicoset",
        connect_timeout=300
    )

    return conn


# Configura a conexão com o banco de dados Neo4j
def connect_to_neo4j():
    conn = GraphDatabase.driver(
        "bolt://127.0.0.1:7687", auth=("neo4j", "songs123#"))

    return conn


# Recupera as músicas do banco de dados MySQL
def fetch_songs_in_mysql():
    conn = connect_to_mysql()
    cursor = conn.cursor()

    query = """
    SELECT s.song_id, s.billboard, l.lyrics
    FROM songs s
    INNER JOIN lyrics l on (s.song_id = l.song_id)
    WHERE l.lyrics IS NOT NULL
    """
    cursor.execute(query)
    songs_data = cursor.fetchall()

    cursor.close()
    conn.close()

    return songs_data


# Remove caractéres especiais, frases adicionais fixadas
# na letra e deixa todo o texto em caixa baixa
def process_lyrics(lyrics):
    # Remover colchetes e seu conteúdo
    lyrics = re.sub(r'\[.*?\]', '', lyrics)
    # Substitui as quebras de linha por espaços em branco
    lyrics = re.sub(r'[\r\n]', ' ', lyrics)
    # Remove caracteres que não sejam letras, números ou espaços em branco
    lyrics = re.sub(r'[^\w\s]', '', lyrics)
    # Converter para letras minúsculas
    lyrics = lyrics.lower()
    # Remover espaços em branco extras
    lyrics = re.sub(r'\s+', ' ', lyrics)

    return lyrics.strip()


# Extrai e formata informações específicas de cada música
def format_songs_data(songs_data):
    formatted_songs = []
    for song in songs_data:
        song_id = song[0]
        billboard = song[1]
        lyrics = song[2]

        # Extrair o nome da música e o nome do artista da coluna billboard
        match = re.search(r"\('([^']*)', '([^']*)'\)", billboard)
        if match:
            song_name = match.group(1)
            artist = match.group(2)
        else:
            song_name = ""
            artist = ""

        formatted_song = (song_id, song_name, artist, process_lyrics(lyrics))
        formatted_songs.append(formatted_song)

    return formatted_songs


# Salva uma lista de músicas no banco de dados MySQL
def save_formatted_songs_on_mysql(formatted_songs):
    conn = connect_to_mysql()
    cursor = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS formatted_songs (
        song_id VARCHAR(22),
        song_name VARCHAR(255),
        artist VARCHAR(255),
        lyrics MEDIUMTEXT
    );
    """
    cursor.execute(create_table_query)

    insert_query = "INSERT INTO formatted_songs (song_id, song_name, artist, lyrics) VALUES (%s, %s, %s, %s);"

    # Define o número de registros em cada lote
    batch_size = 100

    try:
        for i in range(0, len(formatted_songs), batch_size):
            batch = formatted_songs[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            conn.commit()

        print(f"Músicas salvas com sucesso no MySQL!")
    except:
        conn.rollback()
        print(f"Houve um problema ao salvar as músicas no MySQL")

    cursor.close()
    conn.close()


# Salva uma lista de músicas no banco de dados Neo4j
def save_formatted_songs_on_neo4j():
    # Buscar músicas na tabela formatted_songs do MySQL
    conn_mysql = connect_to_mysql()

    with conn_mysql.cursor() as cursor:
        cursor.execute(
            "SELECT song_id, song_name, artist, lyrics FROM formatted_songs")
        songs = cursor.fetchall()

    conn_mysql.close()

    def save_song(song):
        conn = connect_to_neo4j()
        with conn.session() as session:
            session.run(
                """
                MERGE (m:Song {song_id: $song_id, song_name: $song_name, artist: $artist, lyrics: $lyrics})
                WITH m, split($lyrics, ' ') as words
                UNWIND words as word
                MERGE (k:Keyword {keyword: word})
                MERGE (m)-[:CONTAINS_KEYWORD]->(k)
                """,
                song_id=song[0],
                song_name=song[1],
                artist=song[2],
                lyrics=song[3]
            )
        conn.close()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(save_song, songs)


# Com base em uma palavra-chave ou conjunto lírico,  procura por
# músicas que contenham esses caractéres no banco de dados MySQL
def search_songs_in_mysql(keyword):
    conn = connect_to_mysql()
    cursor = conn.cursor()

    # Utilizando o curinga % para procurar a palavra-chave na letra das músicas
    search_query = "SELECT * FROM formatted_songs WHERE lyrics LIKE  %s"
    cursor.execute(search_query, ('%' + keyword + '%',))
    search_results = cursor.fetchall()

    cursor.close()
    conn.close()

    return search_results


# Verifica se existem registros salvos no banco de dados do MySQL
def is_mysql_empty():
    conn = connect_to_mysql()
    cursor = conn.cursor()

    cursor.execute("SHOW TABLES LIKE 'formatted_songs'")
    table_exists = cursor.fetchone()

    if table_exists:
        cursor.execute("SELECT COUNT(*) FROM formatted_songs")
        record_count = cursor.fetchone()[0]
        conn.close()
        return record_count == 0
    else:
        conn.close()
        return False


# Com base em uma palavra-chave ou conjunto lírico,  procura por
# músicas que contenham esses caractéres no banco de dados Neo4j
def search_songs_in_neo4j(keyword):
    conn = connect_to_neo4j()

    with conn.session() as session:
        result = session.run(
            "MATCH (m:Song) "
            "WHERE m.lyrics =~ $regex "
            "RETURN m.song_id, m.song_name, m.artist, m.lyrics",
            regex=f".*{keyword}.*"
        )
        songs = [(record["m.song_id"], record["m.song_name"],
                  record["m.artist"], record["m.lyrics"]) for record in result]

    conn.close()

    return songs


# Verifica se existem registros salvos no banco de dados do neo4j
def is_neo4j_empty():
    conn = connect_to_neo4j()

    with conn.session() as session:
        result = session.run("MATCH (n) RETURN count(n) AS count")
        record = result.single()
        count = record["count"]

    conn.close()

    return count == 0


if is_mysql_empty():
    print(f"A Tabela formatted_songs está vazia. Executando o salvamento das músicas...")
    songs_data = fetch_songs_in_mysql()
    formatted_songs = format_songs_data(songs_data)
    save_formatted_songs_on_mysql(formatted_songs)

if is_neo4j_empty():
    print(f"O banco de dados do Neo4j está vazio. Executando o salvamento das músicas...")
    save_formatted_songs_on_neo4j()

search_type = input("Digite o tipo de pesquisa (mysql ou neo4j): ")
song_fragment = input("Digite o trecho da música: ")

start_time = time.time()

if (search_type.lower() == "mysql"):
    results = search_songs_in_mysql(song_fragment)
else:
    results = search_songs_in_neo4j(song_fragment)

end_time = time.time()
execution_time = end_time - start_time

if len(results) > 0:
    print(
        f"Foram encontrados {len(results)} resultados em {round(execution_time, 2):.2f} segundos.")
    print()

    for result in results:
        print(f"ID da música: {result[0]}")
        print(f"Nome da música: {result[1]}")
        print(f"Artista: {result[2]}")
        print()
else:
    print("Nenhuma música encontrada com a busca fornecida.")
