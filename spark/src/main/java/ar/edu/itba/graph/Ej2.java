package ar.edu.itba.graph;

import java.util.ArrayList;  

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class Ej2 {
	public static void main(String[] args) {
		
		SparkConf spark = new SparkConf().setAppName("Exercise 2");
		JavaSparkContext sparkContext= new JavaSparkContext(spark);

		ArrayList<Integer> localData= new ArrayList<Integer>();
		for(int rec= 10000; rec > 0; rec-=1)
			localData.add(rec);
		
		JavaRDD<Integer> originalRDD = sparkContext.parallelize(localData);
		
		JavaRDD<Integer> matchingRDD = originalRDD.filter( new Function<Integer, Boolean>() {
			private static final long serialVersionUID = 1L;

			public Boolean call(Integer v1) throws Exception {
				return v1 % 2 == 0;
			}
		});
		
		System.out.println("Number of elements = " + originalRDD.count());
		System.out.println("Number of matchings = " + matchingRDD.count());
		System.out.println("Matching Percentage = " + 100 * matchingRDD.count() / originalRDD.count() );
		sparkContext.close();

	}
}