package ar.edu.itba.graph;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.sort_array;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;

import scala.collection.JavaConversions;

public class Ej2 {
    
    public static final String HEADER = "Continent \t Country \t CountryDescription \t Elevations";

    private final GraphFrame airRoutes;

    public Ej2(final GraphFrame airRoutes) {
        this.airRoutes = airRoutes;
    }

    public Dataset<Row> execute() {
        
        return airRoutes
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
    }

    public static String resultToString(final Row row) {
        return row.getAs("continent").toString() + "\t" 
            + row.getAs("country").toString() + "\t" 
            + row.getAs("countryDesc").toString() + "\t" 
            + JavaConversions.asJavaCollection(row.getAs("elevations")).toString()
            ;
    }
}
