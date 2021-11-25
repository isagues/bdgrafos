package ar.edu.itba.graph;

import static ar.edu.itba.graph.GraphUtils.loadEdges;
import static ar.edu.itba.graph.GraphUtils.loadGraphML;
import static ar.edu.itba.graph.GraphUtils.loadVertex;

import java.io.BufferedWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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

public class AirRoutesMain {


    public static void main(String[] args) throws IOException {
        
        //
        final SparkConf spark = new SparkConf().setAppName("TPE - isagues");
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

        // Ejecucion de ambos ejercicios
        final Dataset<Row> ej1 = new Ej1(airRoutes).execute();
        final Dataset<Row> ej2 = new Ej2(airRoutes).execute();


        final String timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd-HHmmss").format(LocalDateTime.now());

        aw.writeAnswer(ej1, Ej1::resultToString, Ej1.HEADER, timestamp + "-b1.txt");
        aw.writeAnswer(ej2, Ej2::resultToString, Ej2.HEADER, timestamp + "-b2.txt");

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


    
    
    
}
