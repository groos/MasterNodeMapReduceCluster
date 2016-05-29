package MapReduce

//#messages
final case class MapJob(key: String, value: String, processFunction: String)
final case class MyList(key: String, value: Int)
final case class StatsJob(text: String)
final case class StatsResult(meanWordLength: Double)
final case class JobFailed(reason: String)
//#messages
