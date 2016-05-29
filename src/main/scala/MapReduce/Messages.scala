package MapReduce

import scala.collection.mutable.HashMap

//#messages
final case class MapJob(key: String, value: String, mapFunction: (String, String) => List[MyTuple], reduceFunction: (List[MyTuple]) => String)

final case class MapReduceResult(results:String)

final case class MyTuple(key: String, value: Int)
final case class StatsJob(text: String)
final case class StatsResult(meanWordLength: Double)
final case class JobFailed(reason: String)
//#messages
