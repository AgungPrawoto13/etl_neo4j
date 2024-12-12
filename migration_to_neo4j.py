from neo4j import GraphDatabase
import psycopg2
import decimal
from itertools import islice

pg_config = {
    'host': '127.0.0.1',
    'database': 'db_clean5',
    'user': 'postgres',
    'password': 'gudkuesen',
    'port': 5432  
}

neo4j_config = {
    'uri': "bolt://localhost:7687",
    'user': "neo4j",
    'password': "password"
}

def chunks(data, size):
    it = iter(data)
    for first in it:
        yield [first] + list(islice(it, size - 1))

def migrate_data_to_neo4j(pg_query):
    try:
        pg_conn = psycopg2.connect(**pg_config)
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(pg_query)
        rows = pg_cursor.fetchall()
        column_names = [desc[0] for desc in pg_cursor.description]  # Ambil nama kolom

        driver = GraphDatabase.driver(neo4j_config['uri'], auth=(neo4j_config['user'], neo4j_config['password']))
        with driver.session() as session:

            if 'events_bc' in pg_query:
                print("Proses events_bc")
                
                for x, chunk in enumerate(chunks(rows, 5000)):
                    batch_data = [sanitize_data(dict(zip(column_names, row))) for row in chunk]
                    print("batch ke", x)
                    session.execute_write(create_node, batch_data)
            else:
                print("Proses links_bc")
                for chunk in chunks(rows, 5000):
                    batch_data = []
                    for row in chunk:
                        data = dict(zip(column_names, row))
                        sanitized_data = sanitize_data(data)

                        if "event1_id" in sanitized_data and "event2_id" in sanitized_data:
                            batch_data.append({
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
        if pg_cursor: pg_cursor.close()
        if pg_conn: pg_conn.close()
        if driver: driver.close()

def create_node(tx, data):
    query = """
    UNWIND $rows AS row
    CREATE (n:Node)
    SET n = row
    """
    tx.run(query, rows=data)

def create_relationships_batch(tx, batch_data):

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
            sanitized[key] = float(value)  
        elif isinstance(value, (list, dict)):
            sanitized[key] = str(value)  
        elif value is None:
            sanitized[key] = None  
        else:
            sanitized[key] = value 
    return sanitized

pg_query = "SELECT * FROM events_bc limit 20000"

migrate_data_to_neo4j(pg_query)