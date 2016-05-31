package MapReduce

import akka.actor.Actor

//#worker
class MapReduceWorker() extends Actor {
  def receive = {    
    case job: MapJob =>
      //println("worker got a mapjob")
      var map = List[MyTuple]()
      map = job.mapFunction(job.key, job.value)
      sender() ! MapReduceRawResult(job.key, map)
  }
}
//#worker