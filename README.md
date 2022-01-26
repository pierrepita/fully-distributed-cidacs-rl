# Fully Distributed CIDACS-RL

The CIDACS-RL is a brazillian record linkage tool suitable to integrate large amount of data with high accuracy. However, its current implementation relies on a ElasticSearch Cluster to distribute the queries and a single node to perform them through Python Multiprocessing lib. This implementation of CIDACS-RL tool can be deployed in a Spark Cluster using all resources available by Jupyter Kernel still using the ElasticSearch cluster, becaming a fully distributed and cluster based solution. It can outperform the legacy version of CIDACS-RL either on multi-node or single node Spark Environment. 


## config.json

### General info

#### index_data
This flag says if the linkage process includes the indexing of a data set into elastic search. Constraints: string, it can assume the values "yes" or "no". 

#### es_index_name
The name of an existing elasticsearch index (if index_data is 'no') or a new one (if index_data is 'yes'). Constraints: string, elasticsearch valid. 

#### es_connect_string
Elasticsearch API address. Constraints: string, URL format.

#### query_size
Number of candidates output for each Elasticsearch query. Constraints: int.

#### cutoff_exact_match
Cutoff point to determine wether a pair is an exact match or not. Constraints: str, number between 0 and 1.

#### null_value
Value to replace missings on both data sets involved. Constraints: string.

#### temp_dir
Directory used to write checkpoints for exact match and non-exact match phases. Constraints: string, fully qualified path. 

#### debug
If it is set as "true", all records found on exact match will be queried again on non-exact match phase. 

<pre><code>
index_data:"no"
es_index_name:"fd-cidacs-rl"
es_connect_string:"http://localhost:9200"
query_size:100
cutoff_exact_match:"0.95"
null_value:"99"
temp_dir:"/path/to/temp_dir/"
debug:"false"
</code></pre>

<hr />
### Information on involved datasets
<hr />

#### datasets_info
##### indexed_dataset






