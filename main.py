import json
import sys
import os
from pyspark.sql import SparkSession


# payload = {
#     "Title": "node-red-spark-json",
#     "Tables": [
#         "airports_data.csv",
#         "aircrafts_data.csv"
#     ],
#     "Methods": [
#         {
#             "type": "map",
#             "expression": "lambda line: dict(zip(['airport_code','airport_name','city','coordinates_lat'], line.split(',')))",
#             "table": "airports_data.csv"
#         },
#         {
#             "type": "filter",
#             "expression": "lambda row: row['airport_code'] == 'YKS'",
#             "table": "airports_data.csv"
#         },
#         {
#             "type": "collect",
#             "table": "airports_data.csv"
#         }
#     ]
# }


print("Inside python script")

# ========
# Set system properties
os.environ['HADOOP_HOME'] = os.path.dirname(os.path.abspath(__file__))
os.environ['HADOOP_CONF_DIR'] = os.path.join(os.environ['HADOOP_HOME'], 'conf')
os.environ['JAVA_HOME'] = os.environ.get('JAVA_HOME', '/usr/lib/jvm/java-11-openjdk-amd64')
os.environ['SPARK_AUTH_SOCKET_TIMEOUT'] = '3600'

# Disable security manager
if not 'SPARK_SUBMIT_OPTS' in os.environ:
    os.environ['SPARK_SUBMIT_OPTS'] = '-Djava.security.manager=allow'
else:
    os.environ['SPARK_SUBMIT_OPTS'] += ' -Djava.security.manager=allow'

# ========
# Import spark
try:
    # Configure Spark with security settings
    spark = SparkSession.\
            builder.\
            appName("pyspark-node-red").\
            master("local[*]").\
            config("spark.executor.memory", "512m").\
            config("spark.driver.host", "localhost").\
            config("spark.driver.bindAddress", "127.0.0.1").\
            config("spark.authenticate", "false").\
            config("spark.ui.enabled", "false").\
            config("spark.network.crypto.enabled", "false").\
            config("spark.dynamicAllocation.enabled", "false").\
            config("spark.hadoop.fs.defaultFS", "file:///").\
            getOrCreate()
            
    # Set log level to minimize output
    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext
    print("Spark initialized successfully")


    # ========
    # Read JSON file
    try:
        # ====================
        # Receive data
        data = sys.stdin.readline().strip()
        payload = json.loads(data)

        # ====================
        # Extracting the tables
        tables = payload["Tables"]
        if not tables:
            print("No tables found in payload")
            sys.exit(1)
            
        print(f"Processing tables: {tables}")

        # ====================
        # Extract rdds
        rdds = {}
        for table_name in tables:
            file_path = os.path.join(os.getcwd(), "datasets", table_name)
            print(f"Reading {table_name} from {file_path}")
            try:
                rdd = sc.textFile(file_path)
                header = rdd.first()
                rdd = rdd.filter(lambda row: row != header)
                rdds[table_name] = rdd
                count = rdd.count()
                print(f"Loaded table aircrafts_data.csv with {count} records")
            except Exception as e:
                print(f"Error loading table {table_name}: {e}")

        # ====================
        # Extract operations
        operations = payload.get("Methods", [])
        if not operations:
            print("No operations found in payload")
            sys.exit(1)
            
        print(f"Processing {len(operations)} operations")

        print(rdds["airports_data.csv"])

        # ====================
        # Process each operation
        results = {}
        for i, curr_op in enumerate(operations):
            op_type = curr_op.get("type")
            op_expr = curr_op.get("expression", "")
            op_table = curr_op.get("table")
            
            print(f"Operation {i+1}: {op_type} on {op_table}")
            
            if not op_table in rdds:
                print(f"Table {op_table} not found in loaded tables")
                continue
                
            # Prepare function for map/filter operations
            if op_type in ["map", "filter", "collect", "count"] and op_expr:
                try:
                    func = eval(op_expr)
                    print(f"Successfully evaluated expression")
                except Exception as e:
                    print(f"Eval error: {e}")
                    continue
            
            # Apply operations
            if op_type == "map" and op_expr:
                rdds[op_table] = rdds[op_table].map(func)
                print(f"Applied map operation")
            
            elif op_type == "filter" and op_expr:
                rdds[op_table] = rdds[op_table].filter(func)
                print(f"Applied filter operation")
            
            elif op_type == "collect":
                result = rdds[op_table].collect()
                print(f"Collected {len(result)} results")
                results[op_table] = result
            
            elif op_type == "count":
                count = rdds[op_table].count()
                print(f"Count result: {count}")
                results[f"{op_table}_count"] = count

        # ====================
        # Output the final results as JSON for Node-RED
        if results:
            output = {"results": results}
            print("Final results:")
            print(json.dumps(output))
        else:
            print("No results collected. Add a 'collect' operation to see results.")
            

    # =========
    except Exception as e:
        print(f"Failed to process data: {e}")
    finally:
        # spark.stop()
        print("Finished process ok")

except Exception as e:
    print(f"Can't initialize SPARK: {e}")