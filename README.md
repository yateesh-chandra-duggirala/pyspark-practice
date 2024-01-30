Apache Spark has its architectural foundation in the resilient distributed dataset (RDD), a read-only multiset of data items distributed over a cluster of machines, that is maintained in a fault-tolerant way. 
The Dataframe API was released as an abstraction on top of the RDD, followed by the Dataset API. 
In Spark 1.x, the RDD was the primary application programming interface (API), but as of Spark 2.x use of the Dataset API is encouraged even though the RDD API is not deprecated. 
The RDD technology still underlies the Dataset API.

Spark and its RDDs were developed in 2012 in response to limitations in the MapReduce cluster computing paradigm, which forces a particular linear dataflow structure on distributed programs: 
  - MapReduce programs read input data from disk,
  - map a function across the data,
  - reduce the results of the map,
  - store reduction results on disk.
    
Spark's RDDs function as a working set for distributed programs that offers a (deliberately) restricted form of distributed shared memory.
