import mysql.connector
import re
import time
from collections import namedtuple
from neo4j import GraphDatabase
from concurrent.futures import ThreadPoolExecutor

Song = namedtuple('Song', ['song_id', 'song_name', 'artist', 'lyrics'])
graph = GraphDatabase.driver(
    "bolt://localhost:7687", auth=("neo4j", "songs123#"))


def treat_lyrics(lyrics):
    # Remover colchetes e seu conteúdo
    lyrics = re.sub(r'\[.*?\]', '', lyrics)

    # Remover caracteres especiais e pontuações
    lyrics = re.sub(r'[^a-zA-Z\s]', '', lyrics)

    # Converter para letras minúsculas
    lyrics = lyrics.lower()

    # Remover espaços em branco extras
    lyrics = re.sub(r'\s+', ' ', lyrics)

    # Retornar a letra tratada
    return lyrics.strip()


def get_songs_from_mysql():
    db = mysql.connector.connect(
        host='localhost',
        user='root',
        password='root',
        database='musicoset'
    )

    songs = []

    cursor = db.cursor()
    query = """
        SELECT spo.song_id, son.billboard, lyr.lyrics
        FROM song_pop spo
        INNER JOIN songs son ON spo.song_id = son.song_id
        INNER JOIN lyrics lyr ON son.song_id = lyr.song_id
        WHERE lyr.lyrics IS NOT NULL;
    """

    cursor.execute(query)

    for song_data in cursor.fetchall():
        song_id, billboard, lyrics = song_data
        song_info = re.findall(r"'([^']*)'", billboard)
        if len(song_info) >= 2:
            song_name, artist = song_info[:2]
        else:
            song_name = song_info[0]
            artist = ""
        lyrics_treated = treat_lyrics(lyrics)
        song = Song(song_id, song_name, artist, lyrics_treated)
        songs.append(song)

    cursor.close()
    db.close()

    return songs


def check_neo4j_empty():
    with graph.session() as session:
        result = session.run("MATCH (s:Song) RETURN count(s) AS count")
        count = result.single()["count"]
        return count == 0


def process_mysql_to_neo4j():
    songs = get_songs_from_mysql()

    if check_neo4j_empty():
        batch_size = 1000
        song_batches = [songs[i:i+batch_size]
                        for i in range(0, len(songs), batch_size)]

        for batch in song_batches:
            save_songs_batch_to_neo4j(batch)
        print("Músicas do MySQL salvas no Neo4j com sucesso.")
    else:
        print("O Neo4j já possui músicas adicionadas. Nenhuma ação realizada.")


def save_song_to_neo4j(song):
    driver = GraphDatabase.driver(
        "bolt://localhost:7687", auth=("neo4j", "songs123#"))
    with driver.session() as session:
        query = """
        MERGE (s:Song {song_id: $song_id})
        SET s.song_name = $song_name, s.artist = $artist, s.lyrics = $lyrics
        WITH s
        UNWIND $keywords AS keyword
        MERGE (k:Keyword {word: keyword})
        MERGE (s)-[:HAS_KEYWORD]->(k)
        """
        session.run(query, song_id=song.song_id, song_name=song.song_name,
                    artist=song.artist, lyrics=song.lyrics, keywords=song.lyrics.split())


def save_songs_batch_to_neo4j(songs):
    with ThreadPoolExecutor(max_workers=10) as executor:
        tasks = [executor.submit(save_song_to_neo4j, song) for song in songs]
        for task in tasks:
            task.result()


def search_music_by_keywords(keywords):
    with graph.session() as session:
        result = session.run(
            """
            MATCH (s:Song)-[:HAS_KEYWORD]->(k:Keyword)
            WHERE k.word IN $keywords
            RETURN s.song_id, s.song_name, s.artist, s.lyrics
            """,
            keywords=keywords
        )
        songs = []
        for row in result:
            song_id, song_name, artist, lyrics = row
            song = Song(song_id, song_name, artist, lyrics)
            songs.append(song)

        return songs


def search_music_by_lyrics(lyrics_search):
    with graph.session() as session:
        result = session.run(
            """
            MATCH (s:Song)
            WHERE s.lyrics CONTAINS $lyrics_search
            RETURN s.song_id, s.song_name, s.artist, s.lyrics
            """,
            lyrics_search=lyrics_search
        )
        songs = []
        for row in result:
            song_id, song_name, artist, lyrics = row
            song = Song(song_id, song_name, artist, lyrics)
            songs.append(song)

        return songs


# Verificar se o Neo4j está vazio e processar o MySQL para o Neo4j
if check_neo4j_empty():
    process_mysql_to_neo4j()
else:
    print("O Neo4j já possui músicas adicionadas. Nenhuma ação realizada.")


search_type = input("Digite o tipo de pesquisa (palavra-chave ou trecho): ")

if search_type.lower() == "palavra-chave":
    keywords = input(
        "Digite as palavras-chave separadas por vírgula: ").split(",")
    results = search_music_by_keywords(keywords)
elif search_type.lower() == "trecho":
    lyrics_search = input("Digite o trecho da composição lírica: ")
    results = search_music_by_lyrics(lyrics_search)
else:
    print("Tipo de pesquisa inválido.")

if len(results) > 0:
    results_len = len(results)
    print(f"Foram encontradas {results_len} músicas")
    print()

    for result in results:
        print(f"ID da música: {result.song_id}")
        print(f"Nome da música: {result.song_name}")
        print(f"Artista: {result.artist}")
        print()
else:
    print("Nenhuma música encontrada com as palavras-chave fornecidas.")
