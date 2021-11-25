package ar.edu.itba.graph;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import ar.edu.itba.graph.models.EdgeProperties;
import ar.edu.itba.graph.models.VertexProperties;
import avro.shaded.com.google.common.base.Function;

public final class GraphUtils {
    
    private GraphUtils() {
    }

    public static Graph loadGraphML(final String path) throws IOException {
        
        final Graph graph = new TinkerGraph();
        final GraphMLReader reader = new GraphMLReader(graph);
     
        final Configuration conf = new Configuration();
        final FileSystem fileSystem = FileSystem.get(conf);

        final FSDataInputStream is = fileSystem.open(new Path(path));

        reader.inputGraph(is);

        return graph;
    }

	public static List<Row> loadVertex(final Iterable<Vertex> vIterator) {
		
        final List<Row> vertices = new ArrayList<>();
		final List<String> identifiers = VertexProperties.VALUES
            .stream()
            .map(VertexProperties::getIdentifier)
            .collect(Collectors.toList())
            ;	
		
        for(final Vertex vertex : vIterator) {
		  final List<Object> properties = identifiers
            .stream()
            .map(vertex::getProperty)
            .collect(Collectors.toList())
            ;
		  properties.add(0, Long.valueOf((String) vertex.getId()));

          // Agregar la Row
		  vertices.add(RowFactory.create(properties.toArray(new Object[0])));
		}

		return vertices;
	}

	public static List<Row> loadEdges(final Iterable<Edge> eIterator) {
		
        final List<Row> edges = new ArrayList<>();
        final List<String> identifiers = EdgeProperties.VALUES
            .stream()
            .map(EdgeProperties::getIdentifier)
            .collect(Collectors.toList())
            ;	
		
		for (final Edge edge : eIterator) {
            final List<Object> properties = identifiers
                .stream()
                .map(edge::getProperty)
                .collect(Collectors.toList())
                ;

            properties.add(0, Long.valueOf((String) edge.getId()));
            properties.add(1, Long.valueOf((String) edge.getVertex(Direction.OUT).getId()));
            properties.add(2, Long.valueOf((String) edge.getVertex(Direction.IN).getId()));

            // Agregar la Row
            edges.add(RowFactory.create(properties.toArray(new Object[0])));
		}

		return edges;
	}
}
