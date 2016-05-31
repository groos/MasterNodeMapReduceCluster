package MapReduce

import scala.concurrent.duration._
import java.util.concurrent.ThreadLocalRandom
import com.typesafe.config.ConfigFactory
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.RelativeActorPath
import akka.actor.RootActorPath
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.MemberStatus
import scala.collection.mutable.HashMap


object MapReduceClient {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("ClusterSystem")
    system.actorOf(Props(classOf[MapReduceClient], "/user/mapReduceService"), "client")
  }
}

class MapReduceClient(servicePath: String, key:String, value:String, mapFunction:(String,String) => List[MyTuple], reduceFunction:(String, List[MyTuple]) => String) extends Actor {
  val cluster = Cluster(context.system)
  
  val servicePathElements = servicePath match {
    case RelativeActorPath(elements) => elements
    case _ => throw new IllegalArgumentException(
      "servicePath [%s] is not a valid relative actor path" format servicePath)
  }

  var nodes = Set.empty[Address]

  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent])
  }
  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }
    
  self ! MapJob(key, value, mapFunction, reduceFunction)

  def receive = {
    case job: MapJob if nodes.nonEmpty =>
      val randomNode = nodes.toIndexedSeq(ThreadLocalRandom.current.nextInt(nodes.size))
      
      val reduceService = context.actorSelection(RootActorPath(randomNode) / servicePathElements)
      
      reduceService ! job
    
    case job: MapJob if nodes.isEmpty =>
      self ! job // resend the job until we have a node to use.
      
    case result: MapReduceResult =>
        println(result.results)
      
    case failed: JobFailed =>
      println(failed)
      
    case state: CurrentClusterState =>
      nodes = state.members.collect {
        case m if m.hasRole("compute") && m.status == MemberStatus.Up => m.address
      }
    case MemberUp(m) if m.hasRole("compute")        => nodes += m.address
    case other: MemberEvent                         => nodes -= other.member.address
    case UnreachableMember(m)                       => nodes -= m.address
    case ReachableMember(m) if m.hasRole("compute") => nodes += m.address
  }
}
