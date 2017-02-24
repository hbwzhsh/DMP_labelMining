package nlp_keyword_matching

import org.apache.spark.Logging

import scala.collection.mutable.ArrayBuffer


/**
 * Created by LinxiaoBai on 2016/5/25.
 */

/**
  * This class constructs Pattern mathching rules in the data set.
  *
  * Input: List[Subject], List[Verb], List[Object], blurry matching supported,
  * Subject, Object can be empty.
  * See Shared Documents for more.
  *
  * Output: Users Qualitfies any matching rules.
  *
  */

class RegX extends Serializable with Logging {

  private val subjectArray:ArrayBuffer[String]=new ArrayBuffer[String]()
  private val verbArray:ArrayBuffer[String]=new ArrayBuffer[String]()
  private val objectArray:ArrayBuffer[String]=new ArrayBuffer[String]()
  private val patArray:ArrayBuffer[Pattern]=new ArrayBuffer[Pattern]()
  private var upperBound="6"
  private var lowerBound="0"



  private def addPattern(pattern: Pattern) ={
    patArray+=pattern
  }

  def ready()={
    //Subject Pattern
    if(objectArray.isEmpty && verbArray.isEmpty){
      for(i<-subjectArray){
        val ret=Pattern.compile(".{0,}"+i+".{0,}")
        patArray+=ret
      }
    }

    //Verb + Object Pattern
    else if(subjectArray.isEmpty){
      for(i <-verbArray){
        for(j<-objectArray){
          val ret=Pattern.compile(".{0,}"+i+".{"+lowerBound+","+upperBound+"}"+j+".{0,}")
          patArray+=ret
        }
      }
    }

    //Subject + Verb pattern
    else if(objectArray.isEmpty){
      for(i<-subjectArray){
        for(j<-verbArray){
          val ret=Pattern.compile(".{0,}"+i+".{"+lowerBound+","+upperBound+"}"+j+".{0,}")
          patArray+=ret
        }
      }
    }

      //Subject + verb + object pattern
    else {
      for(i<-subjectArray){
        for(j<-verbArray){
          for(k<-objectArray) {
            val ret = Pattern.compile(".{0,}" + i + ".{" + lowerBound + "," + upperBound + "}" + j + ".{" + lowerBound + "," + upperBound + "}"+ k + ".{0,}")
            patArray += ret
          }
        }
      }
    }
  }


  def setBound(lower:String="", upper:String="")={
    upperBound=upper
    lowerBound=lower
  }

  def addSubject(in:String): Unit ={
    subjectArray+=in
  }

  def addVerb(in:String): Unit ={
    verbArray+=in
  }

  def addObject(in:String): Unit ={
    objectArray+=in
  }

  def setSubjectArray(in:Array[String]): Unit ={
    in.foreach(x=>addSubject(x))
  }
  def setVerbArray(in:Array[String]): Unit ={
    in.foreach(x=>addVerb(x))
  }
  def setObjectArray(in:Array[String]): Unit ={
    in.foreach(x=>addObject(x))
  }



  def goMatch(in:String):Boolean={
    val inArray=in.split(Array('，','。','？','！','.',',','?','!')) //Get rid of punctuation
    for(i<-inArray){
      for(j<-patArray){
        if(j.matcher(i).matches())
          return true
      }
    }
    return false
  }
}
