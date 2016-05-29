package MapReduce

import akka.actor.Actor

//#worker
class MapReduceWorker() extends Actor {
  var cache = Map.empty[String, Int]
  def receive = {    
    case "testing" => 
      println("worker got testing message")
      sender() ! "worker response"
      
    case job: MapJob =>
      println("worker got a mapjob")
      var map = List[MyTuple]()
      map = job.mapFunction(job.key, job.value)
  }
}
//#worker