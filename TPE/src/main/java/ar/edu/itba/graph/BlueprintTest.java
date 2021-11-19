package ar.edu.itba.graph;

import static ar.edu.itba.graph.GraphUtils.loadGraphML;

import java.io.IOException;
import java.util.stream.StreamSupport;

import com.tinkerpop.blueprints.Graph;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class BlueprintTest {
    
    public static void main(String[] args) throws IOException {
        
        final SparkConf spark = new SparkConf().setAppName("BlueprintTest");
		
        try(final JavaSparkContext sparkContext= new JavaSparkContext(spark)) {

            
            final Graph graph = loadGraphML(args[0]);
            
            long    vertices = StreamSupport.stream(graph.getVertices().spliterator(), false).count();
            long    edges = StreamSupport.stream(graph.getEdges().spliterator(), false).count();
            
            System.out.printf("Vertex count: %d. Edge count: %d.", vertices, edges);
        }
    }
}
