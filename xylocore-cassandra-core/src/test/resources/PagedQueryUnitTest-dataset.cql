create table paged_query_1
(
    partition_key   bigint,
    cluster_key     bigint,
    val             text,
    
    primary key ( partition_key, cluster_key )
);
