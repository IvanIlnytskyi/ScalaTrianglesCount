import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.graphx.lib.{MyTrianglesCounterWrapper, TriangleCount}

object MyMain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Count Triangles").setMaster("local[*]").set("spark.executor.memory","1g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val graph = GraphLoader.edgeListFile(sc, "src/main/resources/followers.txt", true)
      .partitionBy(PartitionStrategy.RandomVertexCut)
    val vertices= TriangleCount.run(graph).vertices
    val triangles_count=vertices.map(_._2).reduce(_ + _)/3
    val my_counter_result=MyTrianglesCounterWrapper.doTheJob(graph)
    System.out.println("*********************************************")
    System.out.println("Triangles count using code from the lecture: "+triangles_count)
    System.out.println("*********************************************")
    System.out.println("Triangles count using my wrapper: "+my_counter_result)
    System.out.println("*********************************************")


  }
}
