package ar.edu.itba.graph;


import static ar.edu.itba.graph.utils.GraphUtils.loadEdges;
import static ar.edu.itba.graph.utils.GraphUtils.loadGraphML;
import static ar.edu.itba.graph.utils.GraphUtils.loadVertex;
import static ar.edu.itba.graph.utils.GraphUtils.printDataset;


import java.io.IOException;
import java.util.List;

import com.tinkerpop.blueprints.Graph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

import ar.edu.itba.graph.models.EdgeProperties;
import ar.edu.itba.graph.models.VertexProperties;
import ar.edu.itba.graph.utils.AnswerWriter;

public class AditionalQueries {
    
    public static void main(String[] args) throws IOException {
        
        //
        final SparkConf spark = new SparkConf().setAppName("TPE - isagues - AditionalQueries");
        final JavaSparkContext sparkContext = new JavaSparkContext(spark);
        final SparkSession session = SparkSession.builder().appName("TPE - isagues").getOrCreate();
        final SQLContext sqlContext = new org.apache.spark.sql.SQLContext(session);
        final AnswerWriter aw = new AnswerWriter(new Path(args[0]).getParent(), FileSystem.get(new Configuration()));

        // Lectura del graphML
        final Graph graphml = loadGraphML(args[0]);

        // Cargado de vertices
        final List<Row> vertices = loadVertex(graphml.getVertices());
        final Dataset<Row> verticesDF = sqlContext.createDataFrame(vertices, VertexProperties.getSchema());

        // Cargado de aristas
        final List<Row> edges = loadEdges(graphml.getEdges());
        final Dataset<Row> edgesDF = sqlContext.createDataFrame(edges, EdgeProperties.getSchema());

        // AirRoutes
        final GraphFrame airRoutes = GraphFrame.apply(verticesDF, edgesDF);

        // Ejecucion de tests
        final Dataset<Row> allAirports = new Ej1(airRoutes).allAirports();
        final Dataset<Row> sumElevations = new Ej2(airRoutes).sumOfElevations();

        printDataset(allAirports);
        printDataset(sumElevations);

        sparkContext.close();
    }
}
