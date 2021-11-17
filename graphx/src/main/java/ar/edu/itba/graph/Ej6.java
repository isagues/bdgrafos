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
import ar.edu.itba.graph.models.YearEdgeProp;
import ar.edu.itba.graph.utils.ParityMapper;
import ar.edu.itba.graph.utils.YearMapper;
import scala.Tuple2;

public class Ej6 {
    
    public static void main(String[] args) throws ParseException, IOException {
		
		SparkConf spark = new SparkConf().setAppName("Exercise 6");
		JavaSparkContext sparkContext = new JavaSparkContext(spark);
		
		JavaRDD<Tuple2<Object, VertexProp>> verticesRDD = sparkContext.objectFile(args[0]);

		JavaRDD<Edge<EdgeProp>> edgesRDD = sparkContext.objectFile(args[1]);

		Graph<VertexProp, EdgeProp> myGraph = Graph.apply(
						verticesRDD.rdd(), 
						edgesRDD.rdd(), 
						new VertexProp("default", "L"), 
						StorageLevel.MEMORY_ONLY(),
						StorageLevel.MEMORY_ONLY(), 
						scala.reflect.ClassTag$.MODULE$.apply(VertexProp.class), 
						scala.reflect.ClassTag$.MODULE$.apply(EdgeProp.class) 
						);

        Graph<ParityVertexProp, EdgeProp> newGraph = myGraph
            .mapVertices(
				new ParityMapper(),
				scala.reflect.ClassTag$.MODULE$.apply(ParityVertexProp.class),
                null
            );
        
        Graph<ParityVertexProp, YearEdgeProp> newGraphv2 = newGraph
            .mapEdges(
				new YearMapper(),
				scala.reflect.ClassTag$.MODULE$.apply(YearEdgeProp.class)
            );   

		logStatus(newGraphv2);

		sparkContext.close();
	}
}
