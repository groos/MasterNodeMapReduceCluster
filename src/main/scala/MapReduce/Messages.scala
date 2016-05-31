package MapReduce

import scala.collection.mutable.HashMap

//#messages
final case class MapJob(key: String, value: String, mapFunction: (String, String) => List[MyTuple], reduceFunction: (String, List[MyTuple]) => String)

final case class MapReduceRawResult(key:String, rawData:List[MyTuple])
final case class MapReduceResult(results:String)
final case class MyTuple(key: String, value: Int)
final case class JobFailed(reason: String)
//#messages
