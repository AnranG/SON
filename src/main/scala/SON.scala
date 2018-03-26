import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.Ordering.Implicits._
import util.control.Breaks._


object SON {
  def main(args: Array[String]): Unit = {
    val t0 = System.nanoTime()
    val conf = new SparkConf()
    conf.setAppName("SON_Apriori")
    conf.setMaster("local[*]")
    conf.set("spark.executor.memory","1g")
    conf.set("driver-memory","4g")
    conf.set("executor-cores","2")

    val case_num = args(0).toInt
    val sup_threshold = args(2).toInt
    if(sup_threshold<=5){
      conf.set("spark.default.parallelism","1")

    }



    val sc = new SparkContext(conf)

    val data = sc.textFile(args(1)).cache()
    val num_partition = data.getNumPartitions

    println("!!!!!!"+num_partition+"!!!!!!")
    var rdd = data.map(line => line.split(","))
    val header = rdd.first()
    rdd = rdd.filter(_ (0) != header(0))


    val rddWCase = bucket_case(rdd,case_num,sup_threshold)

    // (reviewer, list(p1,p2,p3,p4...)) or (product, list(r1,r2,r3))
    val rdd_og = rddWCase.mapValues(line => Set(line)).reduceByKey((p1, p2) => p1.union(p2)).map(line => line._2)

    val phase_1_result = run_phase_1(rdd_og,sup_threshold,num_partition)

    val candidates = phase_1_result.collect().toSet


    println("===phase 1 result ===")
    //    candidates.foreach(println)

    val cdd_bc = sc.broadcast(candidates)
    val phase_2_result = run_phase_2(cdd_bc,rdd_og,sup_threshold)
      .map(line => (line.size,line.toList.sorted))
      .sortBy(x => x._2).map(line =>(line._1, "(\'"+line._2.mkString("\', \'")+"\')"))
      .groupByKey().sortByKey().map(x => x._2.mkString(", "))
      .collect().toList


    phase_2_result.foreach(println)

    var output_fileName = new String
    if(sup_threshold<=5){
      //small2.csv
      output_fileName = "Anran_Guo_SON_"+args(1).charAt(0).toUpper+args(1).split("\\.")(0).substring(1)+".case"+args(0).toString+".txt"
    }else{
      //books or beauty
      output_fileName = "Anran_Guo_SON_"+args(1).charAt(0).toUpper+args(1).split("\\.")(0).substring(1)+".case"+args(0).toString+"-"+sup_threshold.toString+".txt"
    }

    val file = new File(output_fileName)

    val out = new PrintWriter(file)


    for(line <- phase_2_result){
      out.write(line+"\n"+"\n")
    }

    out.close()


    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")



  }
  def bucket_case(rdd: RDD[Array[String]], case_num: Int, sup_threshold: Int): RDD[(String, String)] = {
    if (case_num == 1) {
      val rdd_Case1 = rdd.map(line => (line(0), line(1))).distinct().sortBy(_._1)
      return rdd_Case1
    }
    else {
      val rdd_Case2 = rdd.map(line => (line(1), line(0))).distinct().sortBy(_._1)
      return rdd_Case2
    }

  }

  def run_phase_1(rdd: RDD[Set[String]], sup_threshold:Int, num_ptt:Int):RDD[Set[String]]={


    val sup_ptt = sup_threshold.toFloat/num_ptt
    //phase_1 map

    val result = rdd.mapPartitions{line =>
      var baskets = ListBuffer.empty[(Set[String])]
      line.toList.map(item => baskets+=item).iterator
      apriori(baskets,sup_ptt).iterator
    }


    return result
  }

  def run_phase_2(broadcast: Broadcast[Set[Set[String]]],rdd: RDD[Set[String]],sup_threshold:Int):RDD[(Set[String])] ={
    //
    val result = rdd.mapPartitions {line =>
      var baskets = ListBuffer.empty[(Set[String])]
      line.toList.map(item => baskets += item)
      get_cdd_count(baskets,broadcast.value).iterator
    }.reduceByKey(_+_).filter(line => line._2 >= sup_threshold).map(line => line._1)
    println("!!!")
    result.collect().foreach(println)


    return result
  }

  def get_cdd_count(baskets:ListBuffer[Set[String]],cdd:Set[Set[String]]):List[(Set[String],Int)]  ={
    val result = cdd.map{eachCdd =>
      var count = 0

      baskets.map{eachBasket =>
        if (eachCdd.subsetOf(eachBasket))count += 1
      }
      (eachCdd,count)
    }
    return result.toList
  }


  def apriori(baskets: ListBuffer[Set[String]],sup:Float): Set[Set[String]] ={
    var all_freq_found = Set[Set[String]]()



    val freq_single = baskets.flatten.groupBy(identity).map(item => (item._1,item._2.size))
      .filter(item => item._2>=sup).keysIterator.toList.sorted


    freq_single.map(item =>all_freq_found += Set(item))

    println("=== freq single ===")
    freq_single.foreach(println)


    val freq_pair = freq_single.combinations(2).toList
      .map{item=>
        var count = 0
        baskets.map{eachBasket=>

          if (item.toSet.subsetOf(eachBasket))count += 1
        }

        (item.toSet,count)
      }.filter(item => item._2>=sup).map(item => item._1)


    freq_pair.map(item =>all_freq_found += item)

    println("=== freq pair ===")
    freq_pair.foreach(println)


    var continue = true
    if(freq_pair.size != 0){
      continue = true
    }else{
      continue = false
    }

    var i = 3
    var freq_just_found = freq_pair
    while(continue) {
      println("!!! "+i+"!!!")
      var cdd_item_list = Set.empty[Set[String]]


      //generate cdd item and vaildate
      for(itemA <-freq_just_found ){
        for (itemB<-freq_just_found){
          val new_item = itemA.union(itemB)

          if(new_item.size == i){
            cdd_item_list += new_item
          }

        }
      }
      breakable {
        for (elem <- cdd_item_list) {

          val comb = elem.toList.combinations(i-1)
          for(eachComb <- comb){
            if(freq_just_found.contains(eachComb)){
              cdd_item_list -= elem
              break
            }

          }
        }
      }

      println("=== cdd"+i+" items:===")
      cdd_item_list.foreach(println)



      val freq_item = cdd_item_list.map { item =>
        var count = 0
        baskets.map { eachBasket =>
          //  if (item.forall(eachBasket.contains)) count += 1
          if (item.subsetOf(eachBasket))count += 1
        }

        (item, count)

      }.filter(item => item._2 >= sup).map(item => item._1)




      if(freq_item.size!=0) {
        freq_item.map(item => all_freq_found += item)
        freq_just_found = freq_item.toList
        i += 1

      }else{
        continue = false

      }
      println("=== "+i+"finished ===")




    }
    println("=== ALL ===")
    all_freq_found.foreach(println)
    println("=== ALL END ===")

    return all_freq_found
  }
}
