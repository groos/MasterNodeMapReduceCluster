package MapReduce

import akka.actor.Actor

//#worker
class MapReduceWorker() extends Actor {
    
  var cache = Map.empty[String, Int]
    
  def receive = {    
    case item: String => 
      println("worker got testing message")
      println(item)
      
    case job: MapJob =>
      println("worker got a mapjob")
      var map = List[MyTuple]()
      map = job.mapFunction(job.key, job.value)
      sender() ! map
  }
}
//#worker