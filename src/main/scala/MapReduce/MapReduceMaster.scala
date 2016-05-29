package MapReduce

import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.cluster.singleton.ClusterSingletonManager
import akka.cluster.singleton.ClusterSingletonManagerSettings
import akka.cluster.singleton.ClusterSingletonProxy
import akka.cluster.singleton.ClusterSingletonProxySettings

object MapReduceMaster {
  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      startup(Seq("2551", "2552", "0"))
      MapReduceMasterClient.main(Array.empty)
    } else {
      startup(args)
    }
  }

  def startup(ports: Seq[String]): Unit = {
    ports foreach { port =>
      // Override the configuration of the port when specified as program argument
      val config =
        ConfigFactory.parseString(s"akka.remote.netty.tcp.port=" + port).withFallback(
          ConfigFactory.parseString("akka.cluster.roles = [compute]")).
          withFallback(ConfigFactory.load("mrmaster"))

      val system = ActorSystem("ClusterSystem", config)

      //#create-singleton-manager
      system.actorOf(ClusterSingletonManager.props(
        singletonProps = Props[MapReduceService],
        terminationMessage = PoisonPill,
        settings = ClusterSingletonManagerSettings(system).withRole("compute")),
        name = "mapReduceService")
      //#create-singleton-manager

      //#singleton-proxy
      system.actorOf(ClusterSingletonProxy.props(singletonManagerPath = "/user/mapReduceService",
        settings = ClusterSingletonProxySettings(system).withRole("compute")),
        name = "mapReduceServiceProxy")
      //#singleton-proxy
    }
  }
}

object MapReduceMasterClient {
  def main(args: Array[String]): Unit = {
    // note that client is not a compute node, role not defined
    val system = ActorSystem("ClusterSystem")
    
    def mapFunction(x: String): String = {
    	return x + " Returned From Map Function"
    }
    
    def reduceFunction(): String = {
    	return "Returned from Reduce Function"
    }
      
    val x = "hello!"
    
    // Send map, reduce, and input data to the Map Reducer. There it can be distributed via router.
    system.actorOf(Props(classOf[MapReduceClient], "/user/mapReduceServiceProxy", mapFunction(x), reduceFunction, "put otherrr parameters here"), "client")
  }
}

