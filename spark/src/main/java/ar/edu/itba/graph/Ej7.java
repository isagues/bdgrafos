package ar.edu.itba.graph;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class Ej7 {
	public static void main(String[] args) throws IOException {
		
		SparkConf spark = new SparkConf().setAppName("Exercise 7");
		JavaSparkContext sparkContext= new JavaSparkContext(spark);
		
		System.out.println("Starting.................................................");
		
		JavaRDD<String> originalRDD = sparkContext.textFile(args[0]);
		
		JavaRDD<String> matchingRDD = originalRDD.filter( new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;
			
			public Boolean call(String v1) throws Exception {
				return v1.contains("happiness") || v1.contains("enthusiasm");
			}
		});
		
		System.out.println("Saving.................................................");
		saveRDD(matchingRDD, args[1], args[2]);
		System.out.println("Number of elements = " + originalRDD.count());
		System.out.println("Number of matchings = " + matchingRDD.count());
		System.out.println("Matching Percentage = " + 100 * matchingRDD.count() / originalRDD.count() );
		sparkContext.close();

	}

	public static void saveRDD(final JavaRDD rdd, final String outPath, final String user) throws IOException {

		final Configuration conf = new Configuration();

		try(final FileSystem fs = FileSystem.get(conf)) {
			
			final Path path = new Path(outPath);

			System.out.println("Delete:  " + fs.delete(path, true));
			
			rdd.saveAsTextFile(outPath);

			fs.setOwner(path, user, user);
		}
	}
}