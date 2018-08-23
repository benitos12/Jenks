package com.santander

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._


object validacion_conteo extends Serializable {
 def main (args: Array[String]): Unit ={
   val sparkConf = new SparkConf()
   val sc = new SparkContext(sparkConf)
   val sqlContext = new HiveContext(sc)

   val tabla_staging = args(0)
   val tabla_conteos = args(1)
   val tablaDyA = args(2)
   val tablaDis = args(3)

   val tstaging = sqlContext.read.table(tabla_staging).count
   val tconteos = sqlContext.read.table(tabla_conteos).where(s"nombre_tabla=$tabla_staging").select("nombre_tabla","nreg")
   val resultDyA = tconteos.withColumn("discrepancia",col("nreg")-tstaging)
   resultDyA.drop("nreg").write.format("parquet").mode("Append").saveAsTable(tablaDyA)

   if (resultDyA.rdd.first.getInt(2)==0){
     val resultDis = resultDyA.drop("discrepancia").withColumn("conteo",lit(tstaging))
     resultDis.write.format("parquet").mode("Append").saveAsTable(tablaDis)
   }

 }
}
