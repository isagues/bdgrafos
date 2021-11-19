package ar.edu.itba.graph.models;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public enum EdgeProperties {
    LABELE  ("labelE",  DataTypes.StringType,   false),
    DIST    ("dist",    DataTypes.IntegerType,  true),  
    ;

    public static final List<EdgeProperties>  VALUES = Arrays.asList(values());

    public final String identifier;
    public final DataType dataType;
    public final boolean isNullable;

    private EdgeProperties(final String identifier, final DataType dataType, final boolean isNullable) {
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
        
        final List<StructField> edgeFields = VALUES
            .stream()
            .map(EdgeProperties::toStructField)
            .collect(Collectors.toList())
            ;

        edgeFields.add(0, DataTypes.createStructField("id", DataTypes.LongType, false));
        edgeFields.add(1, DataTypes.createStructField("src", DataTypes.LongType, false));
        edgeFields.add(2, DataTypes.createStructField("dst", DataTypes.LongType, false));

		return DataTypes.createStructType(edgeFields);
    }
}