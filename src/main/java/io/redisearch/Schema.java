package io.redisearch;

import java.util.ArrayList;
import java.util.List;

/**
 * Schema abstracs the schema definition when creating an index.
 * Documents can contain fields not mentioned in the schema, but the index will only index pre-defined fields
 */
public class Schema {
    public enum FieldType {
        Tag("TAG"),
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
        public boolean sortable;
        public boolean noindex;

        public Field(String name, FieldType type, boolean sortable) {
            this.name = name;
            this.type = type;
            this.sortable = sortable;
        }

        public Field(String name, FieldType type, boolean sortable, boolean noindex) {
            this(name, type, sortable);
            this.noindex = noindex;
        }

        public void serializeRedisArgs(List<String> args) {
            args.add(name);
            args.add(type.str);
            if (sortable) {
                args.add("SORTABLE");
            }
            if (noindex) {
                args.add("NOINDEX");
            }
        }
    }

    /**
     * FullText field spec.
     */
    public static class TextField extends Field {

        double weight = 1.0;
        boolean nostem = false;


        public TextField(String name, double weight) {
            super(name, FieldType.FullText, false);
            this.weight = weight;
        }

        public TextField(String name, double weight, boolean sortable) {
            super(name, FieldType.FullText, sortable);
            this.weight = weight;
        }

        public TextField(String name, double weight, boolean sortable, boolean nostem) {
            this(name, weight, sortable);
            this.nostem = nostem;
        }

        public TextField(String name, double weight, boolean sortable, boolean nostem, boolean noindex) {
            this(name, weight, sortable, nostem);
            this.noindex = noindex;
        }

        public TextField(String name) {
            super(name, FieldType.FullText, false);
        }

        @Override
        public void serializeRedisArgs(List<String> args) {
            args.add(name);
            args.add(type.str);
            if (weight != 1.0) {
                args.add("WEIGHT");
                args.add(Double.toString(weight));
            }
            if (sortable) {
                args.add("SORTABLE");
            }
            if (nostem) {
                args.add("NOSTEM");
            }
            if (noindex) {
                args.add("NOINDEX");
            }
        }
    }

    private static class TagField extends Field {
        private static final String DEFAULT_SEPARATOR = ",";
        String separator = DEFAULT_SEPARATOR;

        private TagField(String name) {
            super(name, FieldType.Tag, false);
        }

        private TagField(String name, String separator) {
            this(name);
            this.separator = separator;
        }

        @Override
        public void serializeRedisArgs(List<String> args) {
            args.add(name);
            args.add(type.str);
            if (!separator.equals(DEFAULT_SEPARATOR)) {
                args.add("SEPARATOR");
                args.add(separator);
            }
        }
    }

    public List<Field> fields;

    public Schema() {
        this.fields = new ArrayList<>();

    }

    /**
     * Add a text field to the schema with a given weight
     * @param name the field's name
     * @param weight its weight, a positive floating point number
     * @return the schema object
     */
    public Schema addTextField(String name, double weight) {
        fields.add(new TextField(name, weight));
        return this;
    }

    /**
     * Add a text field that can be sorted on
     * @param name the field's name
     * @param weight its weight, a positive floating point number
     * @return the schema object
     */
    public Schema addSortableTextField(String name, double weight) {
        fields.add(new TextField(name, weight, true));
        return this;
    }

    /**
     * Add a geo filtering field to the schema.
     * @param name the field's name
     * @return the schema object
     */
    public Schema addGeoField(String name) {
        fields.add(new Field(name, FieldType.Geo, false));
        return this;
    }

    /**
     * Add a numeric field to the schema
     * @param name the fields's nam e
     * @return the schema object
     */
    public Schema addNumericField(String name) {
        fields.add( new Field(name, FieldType.Numeric, false));
        return this;
    }

    /* Add a numeric field that can be sorted on */
    public Schema addSortableNumericField(String name) {
        fields.add( new Field(name, FieldType.Numeric, true));
        return this;
    }

    public Schema addTagField(String name) {
        fields.add(new TagField(name));
        return this;
    }

    public Schema addTagField(String name, String separator) {
        fields.add(new TagField(name, separator));
        return this;
    }

    public Schema addField(Field field) {
        fields.add(field);
        return this;
    }

}
