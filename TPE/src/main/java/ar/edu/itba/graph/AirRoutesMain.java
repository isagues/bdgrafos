package ar.edu.itba.graph;

import static ar.edu.itba.graph.GraphUtils.loadEdges;
import static ar.edu.itba.graph.GraphUtils.loadGraphML;
import static ar.edu.itba.graph.GraphUtils.loadVertex;

import java.io.IOException;
import java.util.List;

import com.tinkerpop.blueprints.Graph;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

import ar.edu.itba.graph.models.EdgeProperties;
import ar.edu.itba.graph.models.VertexProperties;

public class AirRoutesMain {
    public static void main(String[] args) throws IOException {
        final SparkConf spark = new SparkConf().setAppName("TPE - isagues");
        final JavaSparkContext sparkContext = new JavaSparkContext(spark);
        final SparkSession session = SparkSession.builder().appName("TPE - isagues").getOrCreate();
        final SQLContext sqlContext = new org.apache.spark.sql.SQLContext(session);

        // Lectura del graphML
        final Graph graphml = loadGraphML(args[0]);

        // Cargado de vertices
        final List<Row> vertices = loadVertex(graphml.getVertices());
        final Dataset<Row> verticesDF = sqlContext.createDataFrame(vertices, VertexProperties.getSchema());

        // Cargado de aristas
        final List<Row> edges = loadEdges(graphml.getEdges());
        final Dataset<Row> edgesDF = sqlContext.createDataFrame(edges, EdgeProperties.getSchema());

        final GraphFrame airRoutes = GraphFrame.apply(verticesDF, edgesDF);

        // printGraph(airRoutes);
        ej1(airRoutes);

        sparkContext.close();
    }

    private static void printGraph(final GraphFrame graph){
        
        System.out.println("Vertices");
        graph.vertices().printSchema();
        graph.vertices().show();

        System.out.println("Edges");
        graph.edges().printSchema();
        graph.edges().show();
    }


    private static void ej1(final GraphFrame airRoutes) {
        
        System.out.println("EJ1");

        final Dataset<Row> oneStop = airRoutes
            .filterVertices("labelV = 'airport'")
            .filterEdges("labelE = 'route'")
            .find("(a)-[e]->(b); (b)-[e2]->(c)")
            .filter(new Column("a.lat").isNotNull().and(new Column("a.lat").lt(Double.valueOf("0"))))
            .filter(new Column("a.lon").isNotNull().and(new Column("a.lon").lt(Double.valueOf("0"))))
            .filter("c.code = 'SEA'")
            .filter("a.id != b.id and a.id != c.id and b.id != c.id")
            ;
            
        final Dataset<Row> oneStopVertex = oneStop.select("a.code", "a.lat", "a.lon", "b.code", "c.code");
        oneStopVertex.show(1000);
        oneStopVertex.printSchema();
        
        
        final Dataset<Row> direct = airRoutes
        .filterVertices("labelV = 'airport'")
        .filterEdges("labelE = 'route'")
        .find("(a)-[e]->(b)")
        .filter(new Column("a.lat").isNotNull().and(new Column("a.lat").lt(Double.valueOf("0"))))
        .filter(new Column("a.lon").isNotNull().and(new Column("a.lon").lt(Double.valueOf("0"))))
        .filter("b.code = 'SEA'")
        .filter("a.id != b.id")
        ;
        
        final Dataset<Row> directVertex = direct.select("a.code", "a.lat", "a.lon", "b.code");
        
        directVertex.show(1000);
        directVertex.printSchema();
    }
}
