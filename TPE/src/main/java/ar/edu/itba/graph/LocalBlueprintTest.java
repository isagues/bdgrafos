package ar.edu.itba.graph;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

public class LocalBlueprintTest {
    public static void main(String[] args) throws IOException {

        final Graph graph = new TinkerGraph();
        final GraphMLReader reader = new GraphMLReader(graph);

        final InputStream is = new FileInputStream(new File("/Users/isagues/facu/bdgrafos/practica/TPE/air-routes.graphml"));

        reader.inputGraph(is);
        
        List<Double> lat = StreamSupport.stream(graph.getVertices().spliterator(), false).map(v -> (Double) v.getProperty("lat")).collect(Collectors.toList());

        long    vertices = StreamSupport.stream(graph.getVertices().spliterator(), false).count();
        long    edges = StreamSupport.stream(graph.getEdges().spliterator(), false).count();
     
        System.out.printf("Vertex count: %d. Edge count: %d.", vertices, edges);
    }
}
