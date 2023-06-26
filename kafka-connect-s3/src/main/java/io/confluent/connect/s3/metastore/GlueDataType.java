package io.confluent.connect.s3.metastore;

import org.apache.kafka.connect.data.Schema;

import java.util.Locale;

public class GlueDataType {

    public static String getDataType(Schema.Type type){
        switch (type){
        case INT8:
        case INT16:
        case INT32:
            return Type.INT.getName();
        case INT64:
            return Type.BIGINT.getName();
        case STRING:
            return Type.STRING.getName();
        case FLOAT32:
            return Type.FLOAT.getName();
        case FLOAT64:
            return Type.DOUBLE.getName();
        }
        return Type.STRING.getName();
    }

    public static enum Type {
        INT,
        BIGINT,
        DOUBLE,
        FLOAT,
        STRING,
        BOOLEAN;

        private String name;

        private Type() {
            this.name = this.name().toLowerCase(Locale.ROOT);
        }

        public String getName() {
            return this.name;
        }
    }
}
