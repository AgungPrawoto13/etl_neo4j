from neo4j import GraphDatabase
import psycopg2
import decimal
from itertools import islice

# Konfigurasi PostgreSQL
pg_config = {
    'host': '127.0.0.1',
    'database': 'db_clean5',
    'user': 'postgres',
    'password': 'gudkuesen',
    'port': 5432  # Sesuaikan jika port berbeda
}

# Konfigurasi Neo4j
neo4j_config = {
    'uri': "bolt://localhost:7687",
    'user': "neo4j",
    'password': "password"
}

# Query untuk PostgreSQL
pg_query = "SELECT * FROM links_bc"

def chunks(data, size):
    it = iter(data)
    for first in it:
        yield [first] + list(islice(it, size - 1))

def fetch_node_by_unique_id(tx, unique_id):
    query = """
    MATCH (n:Node {name: $unique_id})
    RETURN n
    """
    result = tx.run(query, unique_id=unique_id)
    return [record["n"] for record in result]

def migrate_data_to_neo4j(unique_id):
    # Koneksi ke PostgreSQL
    try:
        pg_conn = psycopg2.connect(**pg_config)
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(pg_query)
        rows = pg_cursor.fetchall()
        column_names = [desc[0] for desc in pg_cursor.description]  # Ambil nama kolom

        # Koneksi ke Neo4j
        driver = GraphDatabase.driver(neo4j_config['uri'], auth=(neo4j_config['user'], neo4j_config['password']))
        with driver.session() as session:
            # nodes = session.execute_read(fetch_node_by_unique_id, unique_id)
            # for node in nodes:
            #     print("cek node", node)
            # for row in rows:
            #     data = dict(zip(column_names, row))
            #     sanitized_data = sanitize_data(data)
            #     print("data", data)
            #     session.write_transaction(create_node, sanitized_data)

            if 'events_bc' in pg_query:
                print("Proses events_bc")
                # #insert using batch
                for x, chunk in enumerate(chunks(rows, 5000)):
                    # Format data menjadi list of dictionaries
                    batch_data = [sanitize_data(dict(zip(column_names, row))) for row in chunk]
                    print("batch ke", x)
                    session.execute_write(create_node, batch_data)
            else:
                print("Proses links_bc")
                #create link relation
                for chunk in chunks(rows, 5000):
                # Format data menjadi list of dictionaries
                    batch_data = []
                    for row in chunk:
                        data = dict(zip(column_names, row))
                        sanitized_data = sanitize_data(data)

                        # Pastikan event_1 dan event_2 ada di data
                        if "event1_id" in sanitized_data and "event2_id" in sanitized_data:
                            batch_data.append({
                                # "event_1": sanitized_data["event1_id"],
                                # "event_2": sanitized_data["event2_id"],
                                "properties": {
                                    key: value
                                    for key, value in sanitized_data.items()
                                    if key not in ["link_bc"]
                                }
                            })
                
                    # Buat relationship dalam batch
                    if batch_data:
                        session.execute_write(create_relationships_batch, batch_data)
                        print("Data berhasil dimigrasikan ke Neo4j.")
                    else:
                        print("Data gagal masuk")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Tutup koneksi
        if pg_cursor: pg_cursor.close()
        if pg_conn: pg_conn.close()
        if driver: driver.close()

# Fungsi untuk membuat node di Neo4j
def create_node(tx, data):
    # Query Cypher untuk membuat node
    query = """
    UNWIND $rows AS row
    CREATE (n:Node)
    SET n = row
    """
    tx.run(query, rows=data)

# Fungsi untuk membuat batch relationship
def create_relationships_batch(tx, batch_data):
    # for row in batch_data:
    #     # Periksa apakah `event2_id` ada isinya
    #     print("Cek data row", row)
        # if row.get('event2_id'):  # Menggunakan `get` untuk menghindari KeyError jika kunci tidak ada
        #     print(f"event2_id exists: {row['event2_id']}")
        # else:
        #     print("event2_id is missing or None")

    query = """
    UNWIND $rows AS row
    MATCH (start:Event {unique_id: row.properties.event_1})
    MATCH (end:Event {unique_id: row.properties.event_2})
    MERGE (start)-[r:LINK]->(end)
    SET r += row.properties
    """
    tx.run(query, rows=batch_data)

def sanitize_data(data):
    sanitized = {}
    for key, value in data.items():
        if isinstance(value, decimal.Decimal):
            sanitized[key] = float(value)  # Konversi Decimal ke float
        elif isinstance(value, (list, dict)):
            sanitized[key] = str(value)  # Konversi list/dict ke string
        elif value is None:
            sanitized[key] = None  # Biarkan None tetap None
        else:
            sanitized[key] = value  # Tidak ada perubahan untuk tipe data lain
    return sanitized

# Jalankan migrasi
migrate_data_to_neo4j(unique_id = "TRANS_ZMB_US_R_1050_202407270000000876")