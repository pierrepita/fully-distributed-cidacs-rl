# Fully Distributed CIDACS-RL

The CIDACS-RL is a brazillian record linkage tool suitable to integrate large amount of data with high accuracy. However, its current implementation relies on a ElasticSearch Cluster to distribute the queries and a single node to perform them through Python Multiprocessing lib. This implementation of CIDACS-RL tool can be deployed in a Spark Cluster using all resources available by Jupyter Kernel still using the ElasticSearch cluster, becaming a fully distributed and cluster based solution. It can outperform the legacy version of CIDACS-RL either on multi-node or single node Spark Environment. 


## config.json

### General info

#### index_data
This flag says if the linkage process includes the indexing of a data set into elastic search. It can assume the values "yes" or "no". 
<pre><code>
index_data:"no"
</code></pre>


es_index_name:"fd-cidacs-rl"
es_connect_string:"http://localhost:9200"
query_size:100
cutoff_exact_match:"0.95"
null_value:"99"
temp_dir:"../../../0_global_data/fd-cidacs-rl/temp_dataframe/"
debug:"false"
