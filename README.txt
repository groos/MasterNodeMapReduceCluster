CSC 536
Generic Map Reduce Akka Cluster
Nick Groos
 
Acknowledgements:
This project is based around the architecture and configuration presented in the Lightbend Tutorial: Akka Cluster Samples with Java.
In particular, the project configuration and receive function in MapReducer.scala are directly based on this tutorial.
 
 
README:
This Map Reduce framework is genericized to accept any map and reduce functions with the signatures:
 
mapFunction:(String,String) => List[MyTuple]
reduceFunction:(String, List[MyTuple]) => String
 
 
The 3 map reduce examples requested for this project are shipped from FunctionFactory.scala.
This factory also handles the source content given the respective map reduce functions.
Some basic source content files are included in the root directory.
 
 
TO RUN:
>> sbt
>> runMain MapReduce.MapReduceMaster [map-reduce-enum]
 
Arguments:
map-reduce-enum: an integer 0, 1, or 2:
0 = word count
1 = reverse index
2 = url count