# Fully Distributed CIDACS-RL

The CIDACS-RL is a brazillian record linkage tool suitable to integrate large amount of data with high accuracy. However, its current implementation relies on a ElasticSearch Cluster to distribute the queries and a single node to perform them through Python Multiprocessing lib. This implementation of CIDACS-RL tool can be deployed in a Spark Cluster using all resources available by Jupyter Kernel still using the ElasticSearch cluster, becaming a fully distributed and cluster based solution. It can outperform the legacy version of CIDACS-RL either on multi-node or single node Spark Environment. 


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
 'index_data': 'no',
 'es_index_name': 'fd-cidacs-rl',
 'es_connect_string': 'http://localhost:9200',
 'query_size': 100,
 'cutoff_exact_match': '0.95',
 'null_value': '99',
 'temp_dir': '../../../0_global_data/fd-cidacs-rl/temp_dataframe/',
 'debug': 'false',
 
 'datasets_info': {
    'indexed_dataset': {
        'path': '../../../0_global_data/fd-cidacs-rl/sinthetic-dataset-A.parquet',
        'extension': 'parquet',
        'columns': ['id_cidacs_a', 'nome_a', 'nome_mae_a', 'dt_nasc_a', 'sexo_a'],
        'id_column_name': 'id_cidacs_a',
        'storage_level': 'MEMORY_ONLY',
        'default_paralelism': '16'},
    'tolink_dataset': {
        'path': '../../../0_global_data/fd-cidacs-rl/sinthetic-datasets-b/sinthetic-datasets-b-500000.parquet',
        'extension': 'parquet',
        'columns': ['id_cidacs_b', 'nome_b', 'nome_mae_b', 'dt_nasc_b', 'sexo_b'],
        'id_column_name': 'id_cidacs_b',
        'storage_level': 'MEMORY_ONLY',
        'default_paralelism': '16'},
    'result_dataset': {
        'path': '../0_global_data/result/500000/'}},
        
 'comparisons': {
    'name': {
        'indexed_col': 'nome_a',
        'tolink_col': 'nome_b',
        'must_match': 'true',
        'should_match': 'true',
        'is_fuzzy': 'true',
        'boost': '3.0',
        'query_type': 'match',
        'similarity': 'jaro_winkler',
        'weight': 5.0,
        'penalty': 0.02},
    'mothers_name': {
       'indexed_col': 'nome_mae_a',
       'tolink_col': 'nome_mae_b',
       'must_match': 'true',
       'should_match': 'true',
       'is_fuzzy': 'true',
       'boost': '2.0',
       'query_type': 'match',
       'similarity': 'jaro_winkler',
       'weight': 5.0,
       'penalty': 0.02},
  'birthdate': {
       'indexed_col': 'dt_nasc_a',
       'tolink_col': 'dt_nasc_b',
       'must_match': 'false',
       'should_match': 'true',
       'is_fuzzy': 'false',
       'boost': '',
       'query_type': 'term',
       'similarity': 'hamming',
       'weight': 1.0,
       'penalty': 0.02},
  'sex': {
       'indexed_col': 'sexo_a',
       'tolink_col': 'sexo_b',
       'must_match': 'true',
       'should_match': 'true',
       'is_fuzzy': 'false',
       'boost': '',
       'query_type': 'term',
       'similarity': 'overlap',
       'weight': 3.0,
       'penalty': 0.02}}}
</code></pre>


# Running in a Standalone Spark Cluster
 
Read more: https://github.com/elastic/elasticsearch-hadoop
           https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html
           https://search.maven.org/artifact/org.elasticsearch/elasticsearch-spark-30_2.12
 If you intend to run this tool into a single node Spark environment, consider to include this in you spark-submit or spark-shell command line

<pre><code>
pyspark --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.1.3 --conf spark.es.nodes="localhost" --conf spark.es.port="9200"
</code></pre>
 
If you are running into a Spark Cluster under JupyterHUB kernels, try to add this kernel or edit an existing one: 
 
<pre><code>
{
	 "display_name": "Spark3.3",
	  "language": "python",
	   "argv": [
		     "/opt/bigdata/anaconda3/bin/python",
		       "-m",
		         "ipykernel",
			   "-f",
			     "{connection_file}"
			      ],
			       "env": {
				         "SPARK_HOME": "/opt/bigdata/spark",
					   "PYTHONPATH": "/opt/bigdata/spark/python:/opt/bigdata/spark/python/lib/py4j-0.10.9.2-src.zip",
					     "PYTHONSTARTUP": "/opt/bigdata/spark/python/pyspark/python/pyspark/shell.py",
					       "PYSPARK_PYTHON": "/opt/bigdata/anaconda3/bin/python",
					         "PYSPARK_SUBMIT_ARGS": "--master spark://node1.sparkcluster:7077 --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.1.3 --conf spark.es.nodes=['node1','node2'] --conf spark.es.port='9200' pyspark-shell"
						  }
}
</code></pre>

 
# Some advices for indexed data and queries

<ul>
  <li>Every col should be casted as string (df.withColumn('column', F.col('column').cast(string')))</li>
  <li>Date type columns will not be proper indexed as string, except if some preprocessing step tranform it from yyyy-MM-dd to yyyyMMdd.</li>
  <li>All the nodes of elasticsearch cluster must be included on --packages configuration.</li>
  <li>Term queries are good to well structured variables, such as CPF, dates, CNPJ, etc.</li>
</ul>

