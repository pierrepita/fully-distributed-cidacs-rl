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

*(The rest of the original table and example config remain unchanged.)*

---

## Some advices for indexed data and queries

- Every column should be cast as `string` in Spark:  
  `df.withColumn('column', F.col('column').cast('string'))`
- Date-type columns will not be properly indexed as strings unless transformed from `yyyy-MM-dd` to `yyyyMMdd`.
- All Elasticsearch cluster nodes must be included in the `--packages` configuration.
- Term queries are recommended for well-structured variables (e.g., CPF, dates, CNPJ).
