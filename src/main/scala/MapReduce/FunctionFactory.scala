package MapReduce

import scala.collection.mutable.HashMap

object FunctionFactory {
    
    // 0 = word count
    // 1 = proper names
    // 2 = url count
    var mode = 0
    var map = mapFunctionWc
    var reduce = reduceFunctionWc
    
   def main(args: Array[String]): Unit = {
     setMode(0)
   }
    
   def setMode(m:Int) = {
        m match {
            case 0 => 
                println("type: word count")
                map = mapFunctionWc
                reduce = reduceFunctionWc
            case 1 => 
                println("type: proper name count")
            case 2 => 
                println("type: hyperlink count")
        }
    }
    
    def getMap():(String, String) => List[MyTuple] = {
        return map
    }
    
    def getReduce():(List[MyTuple]) => String = {
        return reduce
    }
    
    val mapFunctionWc = (key:String, content:String) => {
      val STOP_WORDS_LIST = List("a", "am", "an", "and", "are", "as", "at", "be", "do", "go", "if", "in", "is", "it", "of", "on", "the", "to")
      
      var result = List[MyTuple]()
        
      for (word <- content.toLowerCase.split("[\\p{Punct}\\s]+")) 
        if ((!STOP_WORDS_LIST.contains(word))) {
            
            result = MyTuple(word, 1)::result
        }
    
      result
    }
    
    val reduceFunctionWc = (rawResults:List[MyTuple]) => {
        var results = HashMap[String,Int]()
        var stringResult = "\n\n------Map Reduce Results------\n\n"
        
        for (item <- rawResults){
            if (results.contains(item.key)){
                results += (item.key -> (results(item.key) + 1))
            } else {
                results += (item.key -> 1)
            }
        }
        
        for (item <- results){
            stringResult += item + "\n"
        }
        
        stringResult += "\n------------------------------\n\n"
        stringResult
    }
    
    
    
}