package ar.edu.itba.graph;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.graphframes.GraphFrame;

import scala.collection.JavaConversions;

public class Ej1{
    
    public static final String HEADER = "Aeropuerto \t Lat \t Long \t Travel";
    
    private static final Column NEGATIVE_LAT = col("a.lat").isNotNull().and(col("a.lat").lt(Double.valueOf("0")));
    private static final Column NEGATIVE_LON = col("a.lon").isNotNull().and(col("a.lon").lt(Double.valueOf("0")));

    private final GraphFrame airRoutes;

    public Ej1(final GraphFrame airRoutes) {
        this.airRoutes = airRoutes;
    }

    public Dataset<Row> execute() {
        
        System.out.println("Running EJ1");

        final Dataset<Row> oneStop = airRoutes
            .filterVertices("labelV = 'airport'")
            .filterEdges("labelE = 'route'")
            .find("(a)-[e]->(b); (b)-[e2]->(c)")
            .filter(NEGATIVE_LAT)
            .filter(NEGATIVE_LON)
            .filter("c.code = 'SEA'")
            .filter("a.id != b.id and a.id != c.id and b.id != c.id")
            ;
        
        System.out.printf("%d flights with 1 stop.%n", oneStop.count());
                 
        final Dataset<Row> direct = airRoutes
            .filterVertices("labelV = 'airport'")
            .filterEdges("labelE = 'route'")
            .find("(a)-[e]->(b)")
            .filter(NEGATIVE_LAT)
            .filter(NEGATIVE_LON)
            .filter("b.code = 'SEA'")
            .filter("a.id != b.id")
            ;
        
        System.out.printf("%d flights with 1 stop.%n", direct.count());

        return oneStop.select(col("a.code"), col("a.lat"), col("a.lon"), array("a.code", "b.code", "c.code").alias("route"))
            .union(direct.select(col("a.code"), col("a.lat"), col("a.lon"), array("a.code", "b.code").alias("route")))
            ;

    }

    // Probar que el filtro de lat y lon negativo no esten filtrando vuelos directos
    public Dataset<Row> allAirports() {
             
        final int LIMIT = 10;

        final Dataset<Row> oneStop = airRoutes
            .filterVertices("labelV = 'airport'")
            .filterEdges("labelE = 'route'")
            .find("(a)-[e]->(b); (b)-[e2]->(c)")
            .filter("c.code = 'SEA'")
            .filter("a.id != b.id and a.id != c.id and b.id != c.id")
            ;
        

        final Dataset<Row> direct = airRoutes
            .filterVertices("labelV = 'airport'")
            .filterEdges("labelE = 'route'")
            .find("(a)-[e]->(b)")
            .filter("b.code = 'SEA'")
            .filter("a.id != b.id")
            ;
        
        return oneStop.select(col("a.code"), col("a.lat"), col("a.lon"), array("a.code", "b.code", "c.code").alias("route")).limit(LIMIT)
            .union(direct.select(col("a.code"), col("a.lat"), col("a.lon"), array("a.code", "b.code").alias("route")).limit(LIMIT))
            ;

    }

    public static String resultToString(final Row row) {
        return row.getAs("code").toString() + "\t" 
            + row.getAs("lat").toString() + "\t" 
            + row.getAs("lon").toString() + "\t" 
            + JavaConversions.asJavaCollection(row.getAs("route")).toString()
            ;
    }

}
