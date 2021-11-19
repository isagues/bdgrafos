package ar.edu.itba.graph;

import static ar.edu.itba.graph.GraphUtils.loadEdges;
import static ar.edu.itba.graph.GraphUtils.loadGraphML;
import static ar.edu.itba.graph.GraphUtils.loadVertex;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.sort_array;

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

    private static final Column NEGATIVE_LAT = new Column("a.lat").isNotNull().and(new Column("a.lat").lt(Double.valueOf("0")));
    private static final Column NEGATIVE_LON = new Column("a.lon").isNotNull().and(new Column("a.lon").lt(Double.valueOf("0")));

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
        ej2(airRoutes);

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
            .filter(NEGATIVE_LAT)
            .filter(NEGATIVE_LON)
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
            .filter(NEGATIVE_LAT)
            .filter(NEGATIVE_LON)
            .filter("b.code = 'SEA'")
            .filter("a.id != b.id")
            ;
        
        final Dataset<Row> directVertex = direct.select("a.code", "a.lat", "a.lon", "b.code");
        
        directVertex.show(1000);
        directVertex.printSchema();
    }
    
    private static void ej2(final GraphFrame airRoutes) {
        
        final Dataset<Row> ans = airRoutes
            .filterEdges("labelE = 'contains'")
            .find("(c)-[]->(a); (p)-[]->(a)")
            .filter(col("a.labelV").eqNullSafe("airport"))
            .filter(col("c.labelV").eqNullSafe("continent"))
            .filter(col("p.labelV").eqNullSafe("country"))
            .select(
                col("c.desc").alias("continent")
                , col("a.country").alias("country")
                , col("p.desc").alias("countryDesc")
                , col("a.elev").alias("elev")
                )
            .groupBy(col("continent"), col("country"), col("countryDesc"))
            .agg(sort_array(collect_list(col("elev"))).alias("elevations"))
            .sort(col("continent"), col("country"), col("countryDesc"))
            ;
            
        
        ans.show(1000, false);
        ans.printSchema();
    }
}
