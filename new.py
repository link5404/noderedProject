import json
import sys
import os
from pyspark.sql import SparkSession


def process_rdd_node(node, rdd_store):
    """
    Recursively process an RDD node and return the resulting RDD.
    """
    if 'Joined-Rdds' in node:
        rdds = [process_rdd_node(child, rdd_store) for child in node['Joined-Rdds']]
        join_method = next((m for m in node.get("Methods", []) if m["type"] == "join"), None)
        if join_method:
            # For simplicity, assume zip join
            joined_rdd = rdds[0].zip(rdds[1])
            return joined_rdd
        return rdds[0]  # fallback

    tables = node.get("Tables", [])
    rdd = None
    if tables:
        table = tables[0]  # assuming one table per node
        if table not in rdd_store:
            raise Exception(f"Table '{table}' not found in rdd_store.")
        rdd = rdd_store[table]

    # Apply methods
    for method in node.get("Methods", []):
        mtype = method.get("type")
        expr = method.get("expression", "")
        if mtype in ("map", "filter") and expr:
            func = eval(expr)
            rdd = rdd.map(func) if mtype == "map" else rdd.filter(func)
        elif mtype == "collect":
            return rdd.collect()
        elif mtype == "count":
            return rdd.count()

    return rdd


print("Inside updated python script")

# === System Setup ===
os.environ['HADOOP_HOME'] = os.path.dirname(os.path.abspath(__file__))
os.environ['JAVA_HOME'] = os.environ.get('JAVA_HOME', '/usr/lib/jvm/java-11-openjdk-amd64')
os.environ['SPARK_SUBMIT_OPTS'] = os.environ.get('SPARK_SUBMIT_OPTS', '') + ' -Djava.security.manager=allow'

# === Spark Init ===
spark = SparkSession.builder \
    .appName("pyspark-node-red-recursive") \
    .master("local[*]") \
    .config("spark.executor.memory", "512m") \
    .config("spark.ui.enabled", "false") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

try:
    # === Read JSON Payload ===
    raw_input = sys.stdin.readline().strip()
    payload = json.loads(raw_input)

    print("Payload received")

    # === Load Tables ===
    rdd_store = {}
    tables = set()

    def collect_tables(node):
        if 'Tables' in node:
            tables.update(node['Tables'])
        for sub_node in node.get('Joined-Rdds', []):
            collect_tables(sub_node)

    collect_tables(payload)

    print(f"Tables to load: {tables}")
    for table in tables:
        file_path = os.path.join(os.getcwd(), "datasets", table)
        rdd = sc.textFile(file_path)
        header = rdd.first()
        rdd = rdd.filter(lambda line: line != header)
        rdd_store[table] = rdd

    # === Process Root RDD ===
    result = process_rdd_node(payload, rdd_store)

    # === Output ===
    if isinstance(result, list):
        print(json.dumps({"results": result}))
    else:
        print(f"Final RDD result (non-collect): {result}")

except Exception as e:
    print(f"Error during processing: {e}")

finally:
    print("Finished execution.")
