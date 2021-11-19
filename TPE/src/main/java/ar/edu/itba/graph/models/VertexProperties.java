package ar.edu.itba.graph.models;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public enum VertexProperties {
    LABELV      ("labelV",      DataTypes.StringType,   false),
    TYPE        ("type",        DataTypes.StringType,   false),
    CODE        ("code",        DataTypes.StringType,   true),
    ICAO        ("icao",        DataTypes.StringType,   true),
    DESC        ("desc",        DataTypes.StringType,   true),
    REGION      ("region",      DataTypes.StringType,   true),
    RUNWAYS     ("runways",     DataTypes.IntegerType,  true),
    LONGEST     ("longest",     DataTypes.IntegerType,  true),
    ELEV        ("elev",        DataTypes.IntegerType,  true),
    COUNTRY     ("country",     DataTypes.StringType,   true),
    CITY        ("city",        DataTypes.StringType,   true),
    LAT         ("lat",         DataTypes.DoubleType,   true),
    LON         ("lon",         DataTypes.DoubleType,   true),
    AUTHOR      ("author",      DataTypes.StringType,   true),
    DATE        ("date",        DataTypes.StringType,   true)
    ;
    
    public static final List<VertexProperties>  VALUES = Arrays.asList(values());

    public final String identifier;
    public final DataType dataType;
    public final boolean isNullable;

    private VertexProperties(final String identifier, final DataType dataType, final boolean isNullable) {
        this.identifier = identifier;
        this.dataType = dataType;
        this.isNullable = isNullable;
    }

    public String getIdentifier() {
        return identifier;
    }

    public StructField toStructField() {
        return DataTypes.createStructField(identifier, dataType, isNullable);
    }

    public static StructType getSchema() {
        
        final List<StructField> vertFields = VALUES
            .stream()
            .map(VertexProperties::toStructField)
            .collect(Collectors.toList())
            ;

		vertFields.add(0, DataTypes.createStructField("id",  DataTypes.LongType, false));

		return DataTypes.createStructType(vertFields);
    }
}