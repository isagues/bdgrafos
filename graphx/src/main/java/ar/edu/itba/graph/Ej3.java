package ar.edu.itba.graph;

import java.util.ArrayList;   
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class Ej3 {
	
	public static void main(String[] args) {
		
		SparkConf spark = new SparkConf().setAppName("Exercise 2");
		JavaSparkContext sparkContext= new JavaSparkContext(spark);
		
		List<Tuple2<Object, String>> vertices = LoadVertices();
		JavaRDD<Tuple2<Object, String>> verticesRDD = sparkContext.parallelize(vertices);

		List<Edge<String>> edges = LoadEdges();
		JavaRDD<Edge<String>> edgesRDD = sparkContext.parallelize(edges);

		
		scala.reflect.ClassTag<String> stringTag = 
		 		scala.reflect.ClassTag$.MODULE$.apply(String.class);
		 
		Graph<String, String> myGraph = Graph.apply(
						verticesRDD.rdd(), 
						edgesRDD.rdd(), 
						"default", 
						StorageLevel.MEMORY_ONLY(),
						StorageLevel.MEMORY_ONLY(), 
						stringTag, 
						stringTag );

		logStatus(myGraph);

		logStatus(myGraph.reverse());

		sparkContext.close();
	}

	private static void logStatus(final Graph<String, String> graph) {
		// in the driver console:
		System.out.println("vertices:");
		graph.vertices().toJavaRDD().collect().forEach(System.out::println);
		
		System.out.println();
		
		System.out.println("edges:");
		graph.edges().toJavaRDD().collect().forEach(System.out::println);
		
		// in the workers' console:
		System.out.println("vertices:");
		graph.vertices().toJavaRDD().foreach(new VoidFunction<Tuple2<Object, String>>() {
				@Override
				public void call(Tuple2<Object, String> t) throws Exception {
					System.out.println(t);
				}
		});
		 
		System.out.println();
		
		System.out.println("edges:");
		graph.edges().toJavaRDD().foreach(new VoidFunction<Edge<String>>() {
			@Override
			public void call(Edge<String> t) throws Exception {
				System.out.println(t);
			}
		});
	}
	
	
	public static List<Tuple2<Object, String>> LoadVertices() {
		List<Tuple2<Object, String>> vertices= 
				new ArrayList<Tuple2<Object, String>>();
		
	 
		vertices.add(new Tuple2<Object, String>(0l, "0.html"));
		vertices.add(new Tuple2<Object, String>(1l, "1.html"));
		vertices.add(new Tuple2<Object, String>(2l, "2.html"));
		vertices.add(new Tuple2<Object, String>(3l, "3.html"));
		vertices.add(new Tuple2<Object, String>(4l, "4.html"));
		vertices.add(new Tuple2<Object, String>(5l, "5.html"));
		vertices.add(new Tuple2<Object, String>(6l, "6.html"));
		vertices.add(new Tuple2<Object, String>(7l, "7.html"));
		vertices.add(new Tuple2<Object, String>(8l, "8.html"));
		vertices.add(new Tuple2<Object, String>(9l, "9.html"));

		return vertices;
	}
	
	
	public static List<Edge<String>> LoadEdges() {
		ArrayList<Edge<String>> edges = new ArrayList<Edge<String>>();
		
		edges.add(new Edge<String>(1, 0, "refersTo"));
		edges.add(new Edge<String>(1, 2, "refersTo"));
		edges.add(new Edge<String>(2, 0, "refersTo"));
		edges.add(new Edge<String>(3, 2, "refersTo"));
		edges.add(new Edge<String>(4, 3, "refersTo"));
		edges.add(new Edge<String>(3, 6, "refersTo"));
		edges.add(new Edge<String>(6, 4, "refersTo"));
	 	edges.add(new Edge<String>(5, 4, "refersTo"));
	 	edges.add(new Edge<String>(8, 5, "refersTo"));
	 	edges.add(new Edge<String>(7, 6, "refersTo"));
	 	edges.add(new Edge<String>(7, 8, "refersTo"));
	 	edges.add(new Edge<String>(8, 6, "refersTo"));
	 	
	 	return edges;
	}
}