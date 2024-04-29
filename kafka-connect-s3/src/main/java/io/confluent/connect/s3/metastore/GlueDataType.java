package io.confluent.connect.s3.metastore;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

public class GlueDataType {

    private final static String DECIMAL_DATATYPE = "decimal(%s,%s)";
    private final static String DECIMAL_PRECISION = "connect.decimal.precision";
    private final static String BINARY_DATATYPE = "binary";
    private final static String STRUCT_DATATYPE = "struct";
    private static final Logger log = LoggerFactory.getLogger(GlueDataType.class);
    public static String getDataType(Schema schema){
        switch (schema.type()){
        case INT8:
        case INT16:
        case INT32:
        case STRING:
        case BOOLEAN:
        case FLOAT32:
        case FLOAT64:
            return getPrimitiveType(schema.type());
        case INT64:
            return TimestampType.TIMESTAMP.getName(schema)==null?PrimitiveType.BIGINT.getName() : TimestampType.TIMESTAMP.getName(schema);
        case MAP:
            return MapType.MAP.getName(schema.keySchema(), schema.valueSchema());
        case ARRAY:
            return ArrayType.ARRAY.getName(schema.valueSchema());
        case BYTES:
            return getBytesDataType(schema);
        case STRUCT:
            return getStructDatatype(schema);
        }
        return PrimitiveType.STRING.getName();
    }

    private static String getStructDatatype(Schema schema) {
        StringBuilder structDatatype = new StringBuilder(STRUCT_DATATYPE);
        structDatatype.append("<");
        for (Field field: schema.fields()) {
            structDatatype.append(field.name());
            structDatatype.append(":");
            structDatatype.append(getDataType(field.schema()));
            structDatatype.append(",");
        }
        structDatatype.deleteCharAt(structDatatype.length() - 1);
        structDatatype.append(">");
        return structDatatype.toString();
    }

    private static String getBytesDataType(Schema schema) {
        if (Decimal.LOGICAL_NAME.equals(schema.name())) {
            return String.format(DECIMAL_DATATYPE, schema.parameters().get(DECIMAL_PRECISION), schema.parameters().get(Decimal.SCALE_FIELD));
        }
        return BINARY_DATATYPE;
    }

    public enum PrimitiveType {
        INT,
        BIGINT,
        DOUBLE,
        FLOAT,
        STRING,
        BOOLEAN;

        private String name;

        PrimitiveType() {
            this.name = this.name().toLowerCase(Locale.ROOT);
        }

        PrimitiveType(String s) {

        }

        public String getName() {
            return this.name;
        }
    }

    public enum MapType {
        MAP("map<keyType,valueType>");

        public String name;

        MapType(String name) {
            this.name = name;
        }

        public String getName(Schema key, Schema value) {
            String keyType = getPrimitiveType(key.type());
            String valueType = getPrimitiveType(value.type());
            this.name = this.name.replaceAll("keyType", keyType);
            this.name = this.name.replaceAll("valueType", valueType);
            return this.name;
        }

    }

    public enum ArrayType {
        ARRAY("array<type>");

        public String name;

        ArrayType(String name) {
            this.name = name;
        }

        public String getName(Schema value) {
            String valueType = getPrimitiveType(value.type());
            this.name = this.name.replaceAll("type", valueType);
            return this.name;
        }

    }

    public enum TimestampType {
        TIMESTAMP;

        public String name;

        TimestampType() {
            this.name = this.name().toLowerCase(Locale.ROOT);
        }

        public String getName(Schema schema) {
            return schema.name() != null && schema.name().equalsIgnoreCase("org.apache.kafka.connect.data.Timestamp") ?
                    this.name :
                    null;
        }

    }
    public static String getPrimitiveType(Schema.Type t) {
        switch (t) {
        case INT8:
        case INT16:
        case INT32:
            return PrimitiveType.INT.getName();
        case STRING:
            return PrimitiveType.STRING.getName();
        case BOOLEAN:
            return PrimitiveType.BOOLEAN.getName();
        case FLOAT32:
            return PrimitiveType.FLOAT.getName();
        case FLOAT64:
            return PrimitiveType.DOUBLE.getName();
        }
        return PrimitiveType.STRING.getName();
    }

}
