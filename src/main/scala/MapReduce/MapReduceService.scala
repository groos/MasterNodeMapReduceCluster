package MapReduce

import scala.concurrent.duration._
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ReceiveTimeout
import akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope
import akka.routing.FromConfig
import scala.collection.mutable.HashMap

//#service
class MapReduceService extends Actor {
  // This router is used both with lookup and deploy of routees. If you
  // have a router with only lookup of routees you can use Props.empty
  // instead of Props[StatsWorker.class].
  val workerRouter = context.actorOf(FromConfig.props(Props[MapReduceWorker]),
    name = "workerRouter")

  def receive = {
    case job: MapJob => 
      //println("service got a mapjob")
      val senderActor = sender()
      
      val aggregator = context.actorOf(Props(classOf[MapReduceAggregator], job.value.split(" ").length, senderActor, job.reduceFunction))
      
      workerRouter.tell(
        ConsistentHashableEnvelope(job, job.value),
          aggregator
      )
  }
}

class MapReduceAggregator(contentSize: Int, sender: ActorRef, reduceFunction:(String, List[MyTuple]) => String) extends Actor {
    
    def receive = {
        case results: MapReduceRawResult =>
            // run the reduce function and send results back to sender
            // println("got results. processing and sending back home.")
            sender ! MapReduceResult(reduceFunction(results.key, results.rawData))
    }
}

//#service
