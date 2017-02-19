package io.redisearch;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dvirsky on 07/02/16.
 */
public class Schema {

    public enum FieldType {
        FullText("TEXT"),
        Geo("GEO"),
        Numeric("NUMERIC"),;

        final String str;
        FieldType(String text) {
            str = text;
        }
    }

    public static class Field {
        public String name;
        public FieldType type;

        public Field(String name, FieldType type) {
            this.name = name;
            this.type = type;
        }


        public void serializeRedisArgs(List<String> args) {

            args.add(name);
            args.add(type.str);

        }
    }

        /**
     * FullText field spec.
     */
    public static class TextField extends Field {

        double weight = 1.0;

        public TextField(String name, double weight) {
            super(name, FieldType.FullText);
            this.weight = weight;
        }

        public TextField(String name) {
            super(name, FieldType.FullText);
        }

        @Override
        public void serializeRedisArgs(List<String> args) {
            super.serializeRedisArgs(args);
            if (weight != 1.0) {
                args.add("WEIGHT");
                args.add(Double.toString(weight));
            }
        }
    }


    public List<Field> fields;

    public Schema() {

        this.fields = new ArrayList<>();

    }

    public Schema addTextField(String name, double weight) {
        fields.add(new TextField(name, weight));
        return this;
    }

    public Schema addGeoField(String name) {
        fields.add(new Field(name, FieldType.Geo));
        return this;
    }

    public Schema addNumericField(String name) {
        fields.add( new Field(name, FieldType.Numeric));
        return this;
    }


}
