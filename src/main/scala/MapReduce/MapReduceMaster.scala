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

/*

counting the number of occurrences of words in a set of text files (the example used in Lecture 3)

computing the reverse index for proper names in a set of text files (what you did in Homework 3)

computing the number of incoming hyperlinks for each html file in a set of html files (the first step in computing its PageRank)

*/

object MapReduceMasterClient {
  def main(args: Array[String]): Unit = {
    // note that client is not a compute node, role not defined
    val system = ActorSystem("ClusterSystem")
    
    def mapFunction(key: String, content: String): String = {
    
      val STOP_WORDS_LIST = List("a", "am", "an", "and", "are", "as", "at", "be", "do", "go", "if", "in", "is", "it", "of", "on", "the", "to")
      
      var result = List[MyList]()
        
      for (word <- content.toLowerCase.split("[\\p{Punct}\\s]+")) 
        if ((!STOP_WORDS_LIST.contains(word))) {
            
            result = MyList(word, 1) :: result
        }
        
      return result.mkString
    }
    
    def reduceFunction(): String = {
    	return "Returned from Reduce Function"
    }
      
    val x = "hello!"

    var filenames = List("text1.txt")
      
    //val content = scala.collection.mutable.Map[String,String]()
      
    var fileContents = ""
    for (file <- filenames){
      fileContents = Source.fromFile(file).getLines.mkString
        
      //content.put(file, fileContents)
      
      system.actorOf(Props(classOf[MapReduceClient], "/user/mapReduceServiceProxy", file, fileContents, mapFunction(file, fileContents)), "client")
    
      
   }
      
  }
}

