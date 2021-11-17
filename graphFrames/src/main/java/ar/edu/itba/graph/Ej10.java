package ar.edu.itba.graph;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

public class Ej10 {

    public static void main(String[] args) throws ParseException {
		
		SparkConf spark = new SparkConf().setAppName("Exercise 10");
		JavaSparkContext sparkContext= new JavaSparkContext(spark);
		SparkSession session = SparkSession.builder()
			  .sparkContext(sparkContext.sc())
			  .getOrCreate();
	
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(session);
		
		List<Row> vertices = LoadVertices();
		Dataset<Row> verticesDF = sqlContext.createDataFrame( vertices, LoadSchemaVertices() );
	
		List<Row> edges = LoadEdges();
		Dataset<Row> edgesDF = sqlContext.createDataFrame( edges, LoadSchemaEdges() );

		
		GraphFrame myGraph = GraphFrame.apply(verticesDF, edgesDF);
		
        myGraph.vertices().createOrReplaceTempView("v_table"); 
        
        Dataset<Row> newVerticesDF = myGraph.sqlContext().sql(
        "SELECT id, URL, Id%2==0 AS parity from v_table");
         
        GraphFrame newGraph = GraphFrame.apply(newVerticesDF, edgesDF);


		// in the driver console
		System.out.println("Graph:");
		newGraph.vertices().show();
		newGraph.edges().show();
		
		
		System.out.println("Schema");
		newGraph.vertices().printSchema();
		newGraph.edges().printSchema();
		
		System.out.println("Degree");
		newGraph.degrees().show();
		
		System.out.println("Indegree");
		newGraph.inDegrees().show();
		
		System.out.println("Outdegree");
		newGraph.outDegrees().show();
		
		sparkContext.close();


	}

    public static StructType LoadSchemaVertices() {
        // metadata
        List<StructField> vertFields = new ArrayList<StructField>();
        vertFields.add(DataTypes.createStructField("id", DataTypes.LongType, true));
        vertFields.add(DataTypes.createStructField("URL", DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("owner", DataTypes.StringType, true));

        StructType schema = DataTypes.createStructType(vertFields);

        return schema;
    }

    public static StructType LoadSchemaEdges() {
        // metadata
        List<StructField> edgeFields = new ArrayList<StructField>();
        edgeFields.add(DataTypes.createStructField("src", DataTypes.LongType, false));
        edgeFields.add(DataTypes.createStructField("dst", DataTypes.LongType, false));
        edgeFields.add(DataTypes.createStructField("label", DataTypes.StringType, false));
        edgeFields.add(DataTypes.createStructField("creationDate", DataTypes.DateType, true));

        StructType schema = DataTypes.createStructType(edgeFields);

        return schema;
    }

    public static List<Row> LoadVertices() {
        ArrayList<Row> vertices = new ArrayList<Row>();

        for (long i = 0; i < 10; i++)
            vertices.add(RowFactory.create(i, i + ".html", i % 2 == 0 ? "A" : "L"));

        return vertices;
    }

    public static List<Row> LoadEdges() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");

        ArrayList<Row> edges = new ArrayList<Row>();

        edges.add(RowFactory.create(1L, 0L, "refersTo", new java.sql.Date(sdf.parse("10/10/2010").getTime())));
        edges.add(RowFactory.create(1L, 0L, "refersTo", new java.sql.Date(sdf.parse("11/10/2010").getTime()))); // new
                                                                                                                // one
        edges.add(RowFactory.create(1L, 0L, "refersTo", new java.sql.Date(sdf.parse("12/10/2010").getTime()))); // new
                                                                                                                // one

        edges.add(RowFactory.create(1L, 2L, "refersTo", new java.sql.Date(sdf.parse("10/10/2010").getTime())));

        edges.add(RowFactory.create(2L, 0L, "refersTo", new java.sql.Date(sdf.parse("10/10/2010").getTime())));

        edges.add(RowFactory.create(3L, 2L, "refersTo", new java.sql.Date(sdf.parse("6/10/2010").getTime())));
        edges.add(RowFactory.create(3L, 2L, "refersTo", new java.sql.Date(sdf.parse("7/10/2010").getTime()))); // new
                                                                                                               // one
        edges.add(RowFactory.create(3L, 2L, "refersTo", new java.sql.Date(sdf.parse("7/10/2012").getTime()))); // new
                                                                                                               // one

        edges.add(RowFactory.create(3L, 6L, "refersTo", new java.sql.Date(sdf.parse("15/10/2010").getTime())));

        edges.add(RowFactory.create(4L, 3L, "refersTo", new java.sql.Date(sdf.parse("15/10/2010").getTime())));

        edges.add(RowFactory.create(5L, 4L, "refersTo", new java.sql.Date(sdf.parse("21/10/2010").getTime())));
        edges.add(RowFactory.create(6L, 4L, "refersTo", new java.sql.Date(sdf.parse("10/10/2010").getTime())));
        edges.add(RowFactory.create(7L, 6L, "refersTo", new java.sql.Date(sdf.parse("10/10/2010").getTime())));
        edges.add(RowFactory.create(7L, 8L, "refersTo", new java.sql.Date(sdf.parse("15/10/2010").getTime())));
        edges.add(RowFactory.create(8L, 5L, "refersTo", new java.sql.Date(sdf.parse("21/10/2010").getTime())));

        edges.add(RowFactory.create(8L, 6L, "refersTo", new java.sql.Date(sdf.parse("15/10/2010").getTime())));
        edges.add(RowFactory.create(8L, 6L, "refersTo", new java.sql.Date(sdf.parse("18/10/2010").getTime()))); // NEW
                                                                                                                // ONE
        edges.add(RowFactory.create(8L, 6L, "refersTo", new java.sql.Date(sdf.parse("21/10/2010").getTime()))); // NEW
                                                                                                                // ONE
        edges.add(RowFactory.create(8L, 6L, "refersTo", new java.sql.Date(sdf.parse("23/10/2010").getTime()))); // NEW
                                                                                                                // ONE

        return edges;
    }

}