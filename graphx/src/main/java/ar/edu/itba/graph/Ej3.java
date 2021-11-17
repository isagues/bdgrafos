package ar.edu.itba.graph;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;

import ar.edu.itba.graph.models.EdgeProp;
import ar.edu.itba.graph.models.VertexProp;
import scala.Tuple2;

public class Ej3 {
	
	public static void main(String[] args) throws ParseException, IOException {
		
		SparkConf spark = new SparkConf().setAppName("Exercise 3");
		JavaSparkContext sparkContext= new JavaSparkContext(spark);
		
		List<Tuple2<Object, VertexProp>> vertices = LoadVertices();
		JavaRDD<Tuple2<Object, VertexProp>> verticesRDD = sparkContext.parallelize(vertices);

		List<Edge<EdgeProp>> edges = LoadEdges();
		JavaRDD<Edge<EdgeProp>> edgesRDD = sparkContext.parallelize(edges);

		Graph<VertexProp, EdgeProp> myGraph = Graph.apply(
						verticesRDD.rdd(), 
						edgesRDD.rdd(), 
						new VertexProp("default", "L"), 
						StorageLevel.MEMORY_ONLY(),
						StorageLevel.MEMORY_ONLY(), 
						scala.reflect.ClassTag$.MODULE$.apply(VertexProp.class), 
						scala.reflect.ClassTag$.MODULE$.apply(EdgeProp.class) 
						);

		logStatus(myGraph);

		saveRDD(myGraph.vertices().toJavaRDD(), args[0], args[2]);
		
		saveRDD(myGraph.edges().toJavaRDD(), 	args[1], args[2]);

		logStatus(myGraph.reverse());

		sparkContext.close();
	}

	private static <V, E> void logStatus(final Graph<V, E> graph) {
		// in the driver console:
		System.out.println("vertices:");
		graph.vertices().toJavaRDD().collect().forEach(System.out::println);
		
		System.out.println();
		
		System.out.println("edges:");
		graph.edges().toJavaRDD().collect().forEach(System.out::println);
		
		// in the workers' console:
		System.out.println("vertices:");
		graph.vertices().toJavaRDD().foreach(new VoidFunction<Tuple2<Object, V>>() {
				@Override
				public void call(Tuple2<Object, V> t) throws Exception {
					System.out.println(t);
				}
		});
		 
		System.out.println();
		
		System.out.println("edges:");
		graph.edges().toJavaRDD().foreach(new VoidFunction<Edge<E>>() {
			@Override
			public void call(Edge<E> t) throws Exception {
				System.out.println(t);
			}
		});
	}
	
	
	public static List<Tuple2<Object, VertexProp>> LoadVertices() {
		List<Tuple2<Object, VertexProp>> vertices= 
				new ArrayList<Tuple2<Object, VertexProp>>();
		
	 
		vertices.add(new Tuple2<Object, VertexProp>(0l, new VertexProp("0.html", "A")));
		vertices.add(new Tuple2<Object, VertexProp>(1l, new VertexProp("1.html", "L")));
		vertices.add(new Tuple2<Object, VertexProp>(2l, new VertexProp("2.html", "A")));
		vertices.add(new Tuple2<Object, VertexProp>(3l, new VertexProp("3.html", "L")));
		vertices.add(new Tuple2<Object, VertexProp>(4l, new VertexProp("4.html", "A")));
		vertices.add(new Tuple2<Object, VertexProp>(5l, new VertexProp("5.html", "L")));
		vertices.add(new Tuple2<Object, VertexProp>(6l, new VertexProp("6.html", "A")));
		vertices.add(new Tuple2<Object, VertexProp>(7l, new VertexProp("7.html", "L")));
		vertices.add(new Tuple2<Object, VertexProp>(8l, new VertexProp("8.html", "A")));
		vertices.add(new Tuple2<Object, VertexProp>(9l, new VertexProp("9.html", "L")));

		return vertices;
	}
	
	
	public static List<Edge<EdgeProp>> LoadEdges() throws ParseException {
		ArrayList<Edge<EdgeProp>> edges = new ArrayList<Edge<EdgeProp>>();
		
		final SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");

		edges.add(new Edge<EdgeProp>(1, 0, new EdgeProp("refersTo", formatter.parse("10/10/2021"))));
		edges.add(new Edge<EdgeProp>(1, 2, new EdgeProp("refersTo", formatter.parse("10/10/2021"))));
		edges.add(new Edge<EdgeProp>(2, 0, new EdgeProp("refersTo", formatter.parse("10/10/2021"))));
		edges.add(new Edge<EdgeProp>(3, 2, new EdgeProp("refersTo", formatter.parse("10/10/2021"))));
		edges.add(new Edge<EdgeProp>(3, 6, new EdgeProp("refersTo", formatter.parse("15/10/2021"))));
		edges.add(new Edge<EdgeProp>(4, 3, new EdgeProp("refersTo", formatter.parse("15/10/2021"))));
		edges.add(new Edge<EdgeProp>(5, 4, new EdgeProp("refersTo", formatter.parse("21/10/2021"))));
		edges.add(new Edge<EdgeProp>(6, 4, new EdgeProp("refersTo", formatter.parse("10/10/2021"))));
		edges.add(new Edge<EdgeProp>(7, 6, new EdgeProp("refersTo", formatter.parse("10/10/2021"))));
		edges.add(new Edge<EdgeProp>(7, 8, new EdgeProp("refersTo", formatter.parse("15/10/2021"))));
	 	edges.add(new Edge<EdgeProp>(8, 5, new EdgeProp("refersTo", formatter.parse("21/10/2021"))));
	 	edges.add(new Edge<EdgeProp>(8, 6, new EdgeProp("refersTo", formatter.parse("15/10/2021"))));
	 	
	 	return edges;
	}

	public static <T> void saveRDD(final JavaRDD<T> rdd, final String outPath, final String user) throws IOException {

		final Configuration conf = new Configuration();

		try(final FileSystem fs = FileSystem.get(conf)) {
			
			final Path path = new Path(outPath);

			System.out.println("Delete:  " + fs.delete(path, true));
			
			rdd.saveAsObjectFile(outPath);

			fs.setOwner(path, user, user);
		}
	}
}