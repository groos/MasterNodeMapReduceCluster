package MapReduce

import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.cluster.singleton.ClusterSingletonManager
import akka.cluster.singleton.ClusterSingletonManagerSettings
import akka.cluster.singleton.ClusterSingletonProxy
import akka.cluster.singleton.ClusterSingletonProxySettings
import scala.io.Source
import scala.collection.mutable.HashMap

object MapReduceMaster {
  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      startup(Seq("2551", "2552", "0"))
      var a = Array("0")
      MapReduceMasterClient.main(a)
    } else {
      startup(Seq("2551", "2552", "0"))
      MapReduceMasterClient.main(args)
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

/*

counting the number of occurrences of words in a set of text files (the example used in Lecture 3)

computing the reverse index for proper names in a set of text files (what you did in Homework 3)

computing the number of incoming hyperlinks for each html file in a set of html files (the first step in computing its PageRank)

*/

object MapReduceMasterClient {
  def main(args: Array[String]): Unit = {
      
    val system = ActorSystem("ClusterSystem")
    
    // initialize factory
    FunctionFactory.setMode(args(0))
      
    var filenames = FunctionFactory.getFilesList()
    //var filenames = List("text1.txt")
    var map = FunctionFactory.getMap()
    var reduce = FunctionFactory.getReduce()
    
    var clientNumber = 0
    var clientName = "client"
      
    var fileContents = ""
    for (file <- filenames){
      fileContents = Source.fromFile(file).getLines.mkString
    
      system.actorOf(Props(classOf[MapReduceClient], "/user/mapReduceServiceProxy", file, fileContents, map, reduce), clientName + clientNumber)
      
      clientNumber += 1
   }
  }
}

