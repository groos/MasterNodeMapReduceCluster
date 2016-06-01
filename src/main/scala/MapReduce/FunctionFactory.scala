package MapReduce

import scala.collection.mutable.HashMap

object FunctionFactory {
    
    // 0 = word count
    // 1 = reverse index
    // 2 = url count
    var mode = "0"
    
    // Unsafe if no init with main. 
    // Forgot how to properly initalize these types.
    var map = mapFunctionWc
    var reduce = reduceFunctionWc
    
   def main(args: Array[String]): Unit = {
     setMode("0")
   }
    
   def setMode(m:String) = {
       mode = m
        m match {
            case "0" => 
                //println("type: word count")
                map = mapFunctionWc
                reduce = reduceFunctionWc
            case "1" => 
                //println("type: proper name count")
                map = mapFunctionRi
                reduce = reduceFunctionRi
            case "2" => 
                //println("type: hyperlink count")
                map = mapFunctionHyper
                reduce = reduceFunctionHyper
        }
    }
    
    def getMap():(String, String) => List[MyTuple] = {
        return map
    }
    
    def getReduce():(String, List[MyTuple]) => String = {
        return reduce
    }
    
    def getFilesList(): List[String] = {
        mode match {
            case "0" => 
                println("type: word count")
                List("words1.txt", "words2.txt", "words3.txt")
            case "1" => 
                println("type: proper name count")
                List("words1.txt", "words2.txt", "words3.txt")
            case "2" => 
                println("type: hyperlink count")
                List("web1.html", "web2.html", "web3.html")
        }
    }
    
    val mapFunctionHyper = (key:String, content:String) => {
		val href = "href="
		var searching = true
		var startIndex = 0
		var indexOf = -1
		var count = 0
		var result = List[MyTuple]()
        
		while (searching){
            indexOf = content.indexOf(href, startIndex)
			if (indexOf >= 0) {
				result = MyTuple("href", 1)::result
				startIndex = indexOf + 1
			} else {
				searching = false
			}
		}
		
		result
	}

	val reduceFunctionHyper = (key: String, rawResults:List[MyTuple]) => {
		var count = 0
		var stringResult = "\n\n------Map Reduce Results: Hyperlink Count ------\n\n"
        stringResult += "Filename: " + key + "\n\n"
		
		for (item <- rawResults){
			count += 1
		}
		
		stringResult += key + " contains " + count + " hyperlinks."
        stringResult += "\n------------------------------\n\n"
        stringResult
	}

	val mapFunctionRi = (key:String, content:String) => {
		val STOP_WORDS_LIST = List("a", "am", "an", "and", "are", "as", "at", "be", "do", "go", "if", "in", "is", "it", "of", "on", "the", "to")
		
		var result = List[MyTuple]()
		
		for (word <- content.split("[\\p{Punct}\\s]+")) {
		  if (Character.isUpperCase(word.charAt(0))) {
			result = MyTuple(word, 1)::result
		  }
		} 
		
		 result
	}
	
	val reduceFunctionRi = (key:String, rawResults:List[MyTuple]) => {
	    var results = HashMap[String,Int]()
        var stringResult = "\n\n------Map Reduce Results: Reduce Index------\n\n"
        stringResult += "Filename: " + key + "\n\n"
        
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
    
    val mapFunctionWc = (key:String, content:String) => {
      val STOP_WORDS_LIST = List("a", "am", "an", "and", "are", "as", "at", "be", "do", "go", "if", "in", "is", "it", "of", "on", "the", "to")
      
      var result = List[MyTuple]()
        
      for (word <- content.toLowerCase.split("[\\p{Punct}\\s]+")){ 
        if ((!STOP_WORDS_LIST.contains(word))) {
            
            result = MyTuple(word, 1)::result
        }
      }
        
      result
    }
    
    val reduceFunctionWc = (key:String, rawResults:List[MyTuple]) => {
        var results = HashMap[String,Int]()
        var stringResult = "\n\n------Map Reduce Results: Word Count------\n\n"
        stringResult += "Filename: " + key + "\n\n"
        
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