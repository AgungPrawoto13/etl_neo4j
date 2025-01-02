
import pandas as pd
import pytz
import time
import decimal
from datetime import datetime
from tqdm import tqdm
import concurrent.futures
from neo4j import GraphDatabase
from concurrent.futures import ThreadPoolExecutor, as_completed, wait

jakarta_tz = pytz.timezone('Asia/Jakarta')
start_script = datetime.now(jakarta_tz)

# Fungsi untuk membuat koneksi ke Neo4j
class Neo4jHandler:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def execute_query(self, query, parameters=None):
        with self.driver.session() as session:
            session.run(query, parameters)

def chunks_df(data, size):
    """Memecah data menjadi beberapa bagian dengan ukuran tertentu."""

    for i in range(0, len(data), size):
        #print("Cek chunk", i)
        yield data[i:i + size]

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

def add_name_property(batch_data):
    for row in batch_data:
        # Ekstrak 4 karakter terakhir dari unique_id dan tambahkan sebagai properti name
        row["name"] = row["unique_id"][-4:] if "unique_id" in row else None
    return batch_data

# Fungsi untuk membuat node dalam batch
def create_nodes_batch(batch):
    batch_data = [sanitize_data(row) for row in batch]
    batch_data_with_name = add_name_property(batch_data)

    query = """
    UNWIND $batch AS row
    CREATE (n:Node)
    SET n = row
    """

    neo4j_handler.execute_query(query, {"batch": batch_data_with_name})

def create_index_node():
    query_index = """
    CREATE INDEX unique_id_index IF NOT EXISTS
    FOR (n:Node) ON (n.unique_id);
    """
    neo4j_handler.execute_query(query_index, None)

# Fungsi untuk membuat relationships dalam batch
def create_relationships_batch(batch):
    print("Mulai membuat relation")
    query = """
    CALL apoc.periodic.iterate(
        'UNWIND $rows AS row RETURN row',
        'MATCH (a:Node {unique_id: row.properties.event2_id}), (b:Node {unique_id: row.properties.event1_id})
         CREATE (a)<-[r:LINKS_TO]-(b)
         SET r += row.properties',
        {batchSize: 50000, parallel: true, params: {rows: $rows}}
    );
    """
    neo4j_handler.execute_query(query, {"rows": batch})
    print("Selesai membuat relation")

def process_nodes_in_parallel(df, batch_size, max_workers=4):
    print("Membuat node")
    start_event = datetime.now(jakarta_tz)
    batches = chunks_df(df.to_dict(orient="records"), batch_size)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        tqdm(executor.map(create_nodes_batch, batches), 
                  desc="Create node")

    create_index_node()
    end_event = datetime.now(jakarta_tz) - start_event
    return end_event

# Fungsi untuk menjalankan relationship creation secara parallel
def process_relationships_in_parallel(df, batch_size, max_workers=4):
    print("Membuat relasi")
    start_link = datetime.now(jakarta_tz)
    batches = chunks_df(df.to_dict(orient="records"), batch_size)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for chunk in tqdm(batches, desc="Proses create relation"):
            batch_data = [
                {
                    "properties": {
                        key: value
                        for key, value in sanitize_data(row).items()
                        if key not in ["link_bc"]
                    }
                }
                for row in chunk if "event1_id" in row and "event2_id" in row
            ]
            if batch_data:
                futures.append(executor.submit(create_relationships_batch, batch_data))
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                print("Batch selesai cek result", result)
            except Exception as e:
                print("Error:", e)

    end_link = datetime.now(jakarta_tz) - start_link
    return end_link

file = 'testing_file'
df_links = pd.read_parquet(f'{file}/links_bc 1.parquet')[['event1_id','event2_id']]
df_events = pd.read_parquet(f'{file}/events_bc 2.parquet')[['unique_id']]

# Inisialisasi koneksi ke Neo4j
neo4j_config = {
    'uri': "bolt://localhost:7687",
    'user': "neo4j",
    'password': "yourpassword"
}

neo4j_handler = Neo4jHandler(neo4j_config["uri"], neo4j_config["user"], neo4j_config["password"])
# Proses parallel
exc_time_node = process_nodes_in_parallel(df_events, batch_size=50000, max_workers=4)
exc_time_relation = process_relationships_in_parallel(df_links, batch_size=50000, max_workers=4)

# Tutup koneksi
neo4j_handler.close()
end_script = datetime.now(jakarta_tz) - start_script
print("Estimasi script selesai", end_script)
print("Estimasi node selesai", exc_time_node)
print("Estimasi relation selesai", exc_time_relation)
print("selesai bang messi")
