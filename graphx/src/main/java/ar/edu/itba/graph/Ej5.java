package ar.edu.itba.graph;

import static ar.edu.itba.graph.utils.GHelper.logStatus;

import java.io.IOException;
import java.text.ParseException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;

import ar.edu.itba.graph.models.EdgeProp;
import ar.edu.itba.graph.models.ParityVertexProp;
import ar.edu.itba.graph.models.VertexProp;
import ar.edu.itba.graph.utils.ParityMapper;
import scala.Tuple2;

public class Ej5 {
    
    public static void main(String[] args) throws ParseException, IOException {
		
		final SparkConf spark = new SparkConf().setAppName("Exercise 5");
		final JavaSparkContext sparkContext = new JavaSparkContext(spark);
		
		final JavaRDD<Tuple2<Object, VertexProp>> verticesRDD = sparkContext.objectFile(args[0]);

		final JavaRDD<Edge<EdgeProp>> edgesRDD = sparkContext.objectFile(args[1]);

		final Graph<VertexProp, EdgeProp> myGraph = Graph.apply(
						verticesRDD.rdd(), 
						edgesRDD.rdd(), 
						new VertexProp("default", "L"), 
						StorageLevel.MEMORY_ONLY(),
						StorageLevel.MEMORY_ONLY(), 
						scala.reflect.ClassTag$.MODULE$.apply(VertexProp.class), 
						scala.reflect.ClassTag$.MODULE$.apply(EdgeProp.class) 
						);

						final Graph<ParityVertexProp, EdgeProp> newGraph = myGraph
            .mapVertices(
				new ParityMapper(),
				scala.reflect.ClassTag$.MODULE$.apply(ParityVertexProp.class),
                null
            );


		logStatus(newGraph);

		sparkContext.close();
	}
}
