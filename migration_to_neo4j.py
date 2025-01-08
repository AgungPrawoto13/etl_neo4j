
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
        'MATCH (e1:Node {unique_id: row.properties.event1_id}),
               (e2:Node {unique_id: row.properties.event2_id})
         CREATE (e1)<-[b:BACKWARD]-(e2)
         CREATE (e1)-[f:FORWARD]->(e2)
         SET b += row.properties, f += row.properties ',
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

def read_file():
    print("Read file")
    file = 'testing_file'
    query = ""
    df_links = pd.read_parquet(f'{file}/links_bc.parquet')[['event1_id','event2_id']].drop_duplicates()
    df_events = pd.read_parquet(f'{file}/events_bc.parquet')[['unique_id']]
    df_backward = pd.read_parquet(f'{file}/20241114-backward.parquet')

    backward = df_backward[df_backward['bl_number'] == "GAVMNOVE0810245"]
    event_list = backward['event2_id'].to_list() + backward['event1_id'].to_list()

    df_new_link = df_links[
        (df_links['event1_id'].isin(backward['event1_id'])) &
        (df_links['event2_id'].isin(backward['event2_id']))]
    
    df_new_event = df_events[df_events['unique_id'].isin(event_list)]

    print("Success read file")
    return df_new_event, df_new_link

#get data neo4j
def run_query(tx): 
    query = """
    MATCH path1=(n:Node {unique_id: "TRANS_SODOGIDS_R_202410100000147841"})-[:BACKWARD*0..200]->(m1)
    RETURN path1 AS path
    """
    result = tx.run(query)
    return [record["path"] for record in result]

def convert_to_dataframe(paths):
    data = []
    for path in tqdm(paths, desc="Process convert dataframe"):
        if path:
            nodes = path.nodes
            relationships = path.relationships
            for i in range(len(relationships)):
                source = nodes[i]["unique_id"]
                target = nodes[i+1]["unique_id"]
                rel_type = relationships[i].type
                data.append({"source": source, "relation": rel_type, "target": target})
    
    return pd.DataFrame(data)

# Inisialisasi koneksi ke Neo4j
neo4j_config = {
    'uri': "bolt://localhost:7687",
    'user': "neo4j",
    'password': "yourpassword"
}

df_events, df_links = read_file()

neo4j_handler = Neo4jHandler(neo4j_config["uri"], neo4j_config["user"], neo4j_config["password"])
# Proses parallel
exc_time_node = process_nodes_in_parallel(df_events, batch_size=50000, max_workers=4)
exc_time_relation = process_relationships_in_parallel(df_links, batch_size=50000, max_workers=4)

try:
    with neo4j_handler.driver.session() as session:
        print("cek session", session)
        result = session.execute_write(run_query)
        df = convert_to_dataframe(result)
        df = df.drop_duplicates()

    df.to_csv("Hasil backward.csv")

except Exception as e:
    print("Error when convert to df", e)

finally:
    # Tutup koneksi
    neo4j_handler.close()
    
end_script = datetime.now(jakarta_tz) - start_script
print("Estimasi script selesai", end_script)
print("Estimasi node selesai", exc_time_node)
print("Estimasi relation selesai", exc_time_relation)
print("selesai bang messi")
