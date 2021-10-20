package ar.edu.itba.graph;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class Ej4 {
	public static void main(String[] args) {
		
		SparkConf spark = new SparkConf().setAppName("Exercise 4");
		JavaSparkContext sparkContext= new JavaSparkContext(spark);
		
		JavaRDD<String> originalRDD = sparkContext.textFile(args[0]);
		
		JavaRDD<String> matchingRDD = originalRDD.filter( new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;

			public Boolean call(String v1) throws Exception {
				return v1.contains("happiness") || v1.contains("enthusiasm");
			}
		});
		
		System.out.println("Number of elements = " + originalRDD.count());
		System.out.println("Number of matchings = " + matchingRDD.count());
		System.out.println("Matching Percentage = " + 100 * matchingRDD.count() / originalRDD.count() );
		sparkContext.close();

	}
}