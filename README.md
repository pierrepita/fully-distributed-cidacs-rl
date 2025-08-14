# Fully Distributed CIDACS-RL

The CIDACS-RL is a Brazilian record linkage tool suitable for integrating large amounts of data with high accuracy.  
This implementation allows CIDACS-RL to be deployed in a Spark Cluster using all available resources via the Jupyter Kernel while still relying on the Elasticsearch cluster — becoming a fully distributed and cluster-based solution.  
It can outperform the legacy version of CIDACS-RL both in multi-node and single-node Spark environments.

---

## Running on the provided Docker cluster

To run this project using the available Docker-based Big Data cluster, follow these steps:

1. **Clone and start the cluster:**
   ```bash
   git clone https://github.com/pierrepita/cluster_bigdata_linkage.git
   cd cluster_bigdata_linkage
   docker compose up -d
   ```

2. **Access the `lagamar` container via SSH:**
   ```bash
   ssh root@localhost -p 2222  # password: 01
   ```

3. **Set up environment variables inside the container:**
   ```bash
   echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> /root/.bashrc
   echo 'export HADOOP_HOME=/opt/hadoop' >> /root/.bashrc
   echo 'export ES_JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> /root/.bashrc
   echo 'export SPARK_HOME=/opt/spark' >> /root/.bashrc
   echo 'export PATH=$PATH:$HADOOP_HOME/bin:$SPARK_HOME/bin:$JAVA_HOME/bin' >> /root/.bashrc
   source /root/.bashrc
   ```

4. **Create required directories:**
   ```bash
   mkdir temp_dataframe result data
   ```

5. **Install Python dependencies on all nodes:**
   ```bash
   for host in barravento jardimdealah stellamaris lagamar; do
     echo ">>> Installing on $host"
     scp /root/fully-distributed-cidacs-rl/requirements.txt $host:/requirements.txt
     ssh $host "pip3 install -r /requirements.txt"
   done
   ```

6. **Install extra utilities:**
   ```bash
   apt install git p7zip-full -y
   ```

7. **Download and extract example datasets:**
   ```bash
   wget --no-check-certificate 'https://drive.google.com/uc?export=download&id=1TKbsONJuOGLMM6vkkuAQ5gr-IfeM9c_e' -O synthetic-dataset-A.parquet.7z
   wget --no-check-certificate 'https://drive.google.com/uc?export=download&id=1cQTTqCpJw9nFn5J91XqLC8Pk4wFA6OAH' -O synthetic-datasets-b-1000.parquet.7z

   7z x synthetic-dataset-A.parquet.7z -o./synthetic-dataset-A.parquet
   7z x synthetic-datasets-b-1000.parquet.7z -o./synthetic-datasets-b-1000.parquet

   mv -fv synthetic-dataset-A.parquet/sinthetic-dataset-A.parquet/ data/synthetic-dataset-A.parquet
   mv -fv synthetic-datasets-b-1000.parquet/sinthetic-datasets-b-1000.parquet data/synthetic-datasets-b-1000.parquet
   ```

8. **Upload datasets to HDFS:**
   ```bash
   hdfs dfs -put data/ /
   hdfs dfs -mkdir /data/temp_dataframe
   hdfs dfs -mkdir /data/result
   hdfs dfs -mkdir /data/spark-checkpoints
   ```

9. **Run the experiments:**
   - Open **Jupyter Notebook** in your browser at `http://localhost:8000`
   - Open `cidacs-rl.ipynb`
   - Update your `config.json` as needed
   - Execute the notebook cells to run the full linkage process

---

## config.json

Almost all aspects of the linkage can be manipulated by the `config.json` file.  

# config.json

Almost all the aspects of the linkage can be manipulated by the config.json file. 

|    Section    |   Sub-section   |             Field (datatype<valid values>)            | Field description                                                                                                                                           |
|:-------------:|:---------------:|:-----------------------------------------------------:|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| General info  |                 | index_data (str<'yes', 'no'>)                         | This flag says if the linkage process includes the indexing of a data set into elastic search. Constraints: string, it can assume the values "yes" or "no". |
| General info  |                 | es_index_name (str<ES_VALID_INDEX>)                   | The name of an existing elasticsearch index (if index_data is 'no') or a new one (if index_data is 'yes'). Constraints: string, elasticsearch valid.        |
| General info  |                 | es_connect_string (str<ES_URL:ES_PORT>)               | Elasticsearch API address. Constraints: string, URL format.                                                                                                 |
| General info  |                 | query_size (int)                                      | Number of candidates output for each Elasticsearch query. Constraints: int.                                                                                 |
| General info  |                 | cutoff_exact_match (str<0:1 number>)                  | Cutoff point to determine wether a pair is an exact match or not. Constraints: str, number between 0 and 1.                                                 |
| General info  |                 | null_value (str)                                      | Value to replace missings on both data sets involved. Constraints: string.                                                                                  |
| General info  |                 | temp_dir (str<SO path>)                               | Directory used to write checkpoints for exact match and non-exact match phases. Constraints: string, fully qualified path.                                  |
| General info  |                 | debug (str<'true', 'false'>)                          | If it is set as "true", all records found on exact match will be queried again on non-exact match phase.                                                    |
| Datasets info | Indexed dataset | path (str<SO path>)                                   | Path for csv or parquet folder of dataset to index.                                                                                                         |
| Datasets info | Indexed dataset | extension (str<'csv', 'parquet'>)                     | String to determine the type of data reading on Spark.                                                                                                      |
| Datasets info | Indexed dataset | columns (list)                                        | Python list with column names involved on linkage.                                                                                                          |
| Datasets info | Indexed dataset | id_column_name (str)                                  | Name of id column.                                                                                                                                          |
| Datasets info | Indexed dataset | storage_level (str<'MEMORY_AND_DISK', 'MEMORY_ONLY'>) | Directive for memory allocation on Spark.                                                                                                                   |
| Datasets info | Indexed dataset | default_paralelism (str<4*N_OF_AVAILABLE_CORES>)      | Number of partitions of a given Spark dataframe.                                                                                                            |
| Datasets info | tolink dataset  | path (str<SO path>)                                   | Path for csv or parquet folder of dataset to index.                                                                                                         |
| Datasets info | tolink dataset  | extension (str<'csv', 'parquet'>)                     | String to determine the type of data reading on Spark.                                                                                                      |
| Datasets info | tolink dataset  | columns (list)                                        | Python list with column names involved on linkage.                                                                                                          |
| Datasets info | tolink dataset  | id_column_name (str)                                  | Name of id column.                                                                                                                                          |
| Datasets info | tolink dataset  | storage_level (str<'MEMORY_AND_DISK', 'MEMORY_ONLY'>) | Directive for memory allocation on Spark.                                                                                                                   |
| Datasets info | tolink dataset  | default_paralelism (str<4*N_OF_AVAILABLE_CORES>)      | Number of partitions of a given Spark dataframe.                                                                                                            |
| Datasets info | result dataset  | path (str<SO path>)                                   | Path for csv or parquet folder of dataset to index.                                                                                                         |
| Comparisons   | label1          | indexed_col (str)                                     | Name of first column to be compared on indexed dataset                                                                                                      |
| Comparisons   | label1          | tolink_col (str)                                      | Name of first column to be compared on tolink dataset                                                                                                       |
| Comparisons   | label1          | must_match (str<'true', 'false'>)                     | Set if this pair of columns are included on exact match phase                                                                                               |
| Comparisons   | label1          | should_match (str<'true', 'false'>)                   | Set if this pair of columns are included on non-exact match phase                                                                                           |
| Comparisons   | label1          | is_fuzzy (str<'true', 'false'>)                       | Set if this pair of columns are included on fuzzy queries for non-exact match phase                                                                         |
| Comparisons   | label1          | boost (str<weight number>)                            | Set the boost/weight of this pair of columns on queries                                                                                                     |
| Comparisons   | label1          | query_type (str<'match', 'term'>)                     | Set the type of matching for this pair of columns on non-exact match phase                                                                                  |
| Comparisons   | label1          | similarity (str<'jaro_winkler', 'overlap', 'hamming'> | Set the similarity to be calculated between the values of this pair of columns                                                                              |
| Comparisons   | label1          | weight (str<weight number>)                           | Set the weight of this pair of columns.                                                                                                                     |
| Comparisons   | label1          | penalty (str<weight number>)                          | Set the penalty of the overall similarity in case of missing value(s).                                                                                      |
| Comparisons   | label2          | ...                                                   | ...                                                                                                                                                         |




## config.json example

<pre><code>
{
    "index_data": "no",
    "es_index_name": "fd-cidacs-rl",
    "es_connect_string": "http://localhost:9200",
    "query_size": 50,
    "cutoff_exact_match": "0.95",
    "null_value": "99",
    "temp_dir": "hdfs://barravento:9000/data/temp_dataframe/",
    "debug": "false",
    "write_checkpoint": "false",
    "datasets_info": {
                        "indexed_dataset": {
                                            "path": "hdfs://barravento:9000/data/synthetic-dataset-A.parquet",
                                            "extension": "parquet",
                                            "columns": ["id_cidacs_a", "nome_a", "nome_mae_a", "dt_nasc_a", "sexo_a"],
                                            "id_column_name": "id_cidacs_a", 
                                            "storage_level": "MEMORY_ONLY",
                                            "default_paralelism": "16"
                        },
                        "tolink_dataset": {
                                            "path": "hdfs://barravento:9000/data/synthetic-datasets-b-1000.parquet",
                                            "extension": "parquet",
                                            "columns": ["id_cidacs_b", "nome_b", "nome_mae_b", "dt_nasc_b", "sexo_b"],
                                            "id_column_name": "id_cidacs_b",
                                            "storage_level": "MEMORY_ONLY",
                                            "default_paralelism": "16"
                        },
                        "result_dataset": {
                                            "path": "hdfs://barravento:9000/data/result/"
                        }                        
                     },
    "comparisons": {
                        "name": {
                                    "indexed_col": "nome_a",
                                    "tolink_col": "nome_b",
                                    "must_match": "true",
                                    "should_match": "true",
                                    "is_fuzzy": "true",
                                    "boost": "3.0",
                                    "query_type": "match",
                                    "similarity": "jaro_winkler",
                                    "weight": 5.0,
                                    "penalty": 0.02
                        },
                        "mothers_name": {
                                    "indexed_col": "nome_mae_a",
                                    "tolink_col": "nome_mae_b",
                                    "must_match": "true",
                                    "should_match": "true",
                                    "is_fuzzy": "true",
                                    "boost": "2.0",
                                    "query_type": "match",
                                    "similarity": "jaro_winkler",
                                    "weight": 5.0,
                                    "penalty": 0.02
                        },
                        "birthdate": {
                                    "indexed_col": "dt_nasc_a",
                                    "tolink_col": "dt_nasc_b",
                                    "must_match": "false",
                                    "should_match": "true",
                                    "is_fuzzy": "false",
                                    "boost": "",
                                    "query_type": "term",
                                    "similarity": "hamming",
                                    "weight": 1.0,
                                    "penalty": 0.02
                        },
                        "sex": {
                                    "indexed_col": "sexo_a",
                                    "tolink_col": "sexo_b",
                                    "must_match": "true",
                                    "should_match": "true",
                                    "is_fuzzy": "false",
                                    "boost": "",
                                    "query_type": "term",
                                    "similarity": "overlap",
                                    "weight": 3.0,
                                    "penalty": 0.02
                                  }
                    }
}
</code></pre>

---

## Some advices for indexed data and queries

- Every column should be cast as `string` in Spark:  
  `df.withColumn('column', F.col('column').cast('string'))`
- Date-type columns will not be properly indexed as strings unless transformed from `yyyy-MM-dd` to `yyyyMMdd`.
- All Elasticsearch cluster nodes must be included in the `--packages` configuration.
- Term queries are recommended for well-structured variables (e.g., CPF, dates, CNPJ).
