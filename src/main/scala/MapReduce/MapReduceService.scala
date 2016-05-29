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
      println("service got a mapjob")
      val senderActor = sender()
      
      val aggregator = context.actorOf(Props(classOf[MapReduceAggregator], job.value.split(" ").length, senderActor, job.reduceFunction))
      
      
//      for (item <- job.value.split(" ")){
//        workerRouter.tell(
//        ConsistentHashableEnvelope(job, item),
//          aggregator
//        )
//      }
      workerRouter.tell(
        ConsistentHashableEnvelope(job, job.value),
          aggregator
        )

    
    case result: MyTuple => println("hi")
      
    case StatsJob(text) if text != "" =>
      val words = text.split(" ")
      val replyTo = sender() // important to not close over sender()
      // create actor that collects replies from workers
      val aggregator = context.actorOf(Props(
        classOf[StatsAggregator], words.size, replyTo))
      words foreach { word =>
        workerRouter.tell(
          ConsistentHashableEnvelope(word, word), aggregator)
      }
  }
}

class MapReduceAggregator(contentSize: Int, sender: ActorRef, reduceFunction:(List[MyTuple]) => String) extends Actor {
    var results:HashMap[String,Int] = HashMap()
    
    def receive = {
        case results: List[MyTuple] =>
            // run the reduce function and send results back to sender
            println("got results. processing and sending back home.")
            sender ! MapReduceResult(reduceFunction(results))
        
        case "worker response" => println("response received at aggregator")
        case "testMessage" => println("hi from aggregator")
    }
}

class StatsAggregator(expectedResults: Int, replyTo: ActorRef) extends Actor {
  var results = IndexedSeq.empty[Int]
  context.setReceiveTimeout(3.seconds)

  def receive = {
    case wordCount: Int =>
      results = results :+ wordCount
      if (results.size == expectedResults) {
        val meanWordLength = results.sum.toDouble / results.size
        replyTo ! StatsResult(meanWordLength)
        context.stop(self)
      }
    case ReceiveTimeout =>
      replyTo ! JobFailed("Service unavailable, try again later")
      context.stop(self)
  }
}
//#service
