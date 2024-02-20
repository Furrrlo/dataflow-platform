- distributed file system complexity (what exactly it means, cause it can 
  become very complex)
- fault tolerance in batching, without being able to store partial results
- including the need to pass data around: the optimal approach would be to
  condense all operators except the reduce, but does it respect the requisites?
- does partition mean we just split the data in the coordinator and send it to
  n different workers where n is the number of partitions?