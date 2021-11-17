package ar.edu.itba.graph.utils;

import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;

import scala.Tuple2;

public final class GHelper {
    
    public static <V, E> void logStatus(final Graph<V, E> graph) {
		// in the driver console:
		System.out.println("vertices:");
		graph.vertices().toJavaRDD().collect().forEach(System.out::println);
		
		System.out.println();
		
		System.out.println("edges:");
		graph.edges().toJavaRDD().collect().forEach(System.out::println);
		
		// in the workers' console:
		
		graph.vertices().toJavaRDD().foreach(new VoidFunction<Tuple2<Object, V>>() {
				@Override
				public void call(Tuple2<Object, V> t) throws Exception {
					System.out.print("vertices: ");
					System.out.println(t);
				}
		});
				
		graph.edges().toJavaRDD().foreach(new VoidFunction<Edge<E>>() {
			@Override
			public void call(Edge<E> t) throws Exception {
				System.out.print("edges: ");
				System.out.println(t);
			}
		});
	}
}
