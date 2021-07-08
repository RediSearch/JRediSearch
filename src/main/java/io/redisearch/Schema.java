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
        private final FieldName fieldName;
        public final String name;
        public final FieldType type;
        public final boolean sortable;
        public final boolean noindex;

        public Field(String name, FieldType type, boolean sortable) {
            this(name, type, sortable, false);
        }

        public Field(String name, FieldType type, boolean sortable, boolean noindex) {
            this(FieldName.of(name), type, sortable, noindex);
        }

        public Field(FieldName name, FieldType type) {
            this(name, type, false, false);
        }

        public Field(FieldName name, FieldType type, boolean sortable, boolean noIndex) {
            this.fieldName = name;
            this.name = this.fieldName.getName();
            this.type = type;
            this.sortable = sortable;
            this.noindex = noIndex;
        }

        /**
         * Sub-classes should not override this method.
         * @param args 
         */
        public void serializeRedisArgs(List<String> args) {
            this.fieldName.addCommandEncodedArguments(args);
            args.add(type.str);
            serializeTypeArgs(args);
            if (sortable) {
                args.add("SORTABLE");
            }
            if (noindex) {
                args.add("NOINDEX");
            }
        }

        /**
         * Sub-classes should override this method.
         * @param args
         */
        protected void serializeTypeArgs(List<String> args) { }

        @Override public String toString() {
            return "Field{name='" + name + "', type=" + type + ", sortable=" + sortable + ", noindex=" + noindex + "}";
        }
    }

    /**
     * FullText field spec.
     */
    public static class TextField extends Field {

        private final double weight;
        private final boolean nostem;
        private final String phonetic;

        public TextField(String name) {
            this(name, 1.0);
        }

        public TextField(FieldName name) {
            this(name, 1.0, false, false, false, null);
        }

        public TextField(String name, double weight) {
            this(name, weight, false);
        }

        public TextField(String name, double weight, boolean sortable) {
            this(name, weight, sortable, false);
        }

        public TextField(String name, double weight, boolean sortable, boolean nostem) {
            this(name, weight, sortable, nostem, false);
        }

        public TextField(String name, double weight, boolean sortable, boolean nostem, boolean noindex) {
            this(name, weight, sortable, nostem, noindex, null);
        }

        public TextField(String name, double weight, boolean sortable, boolean nostem, boolean noindex, String phonetic) {
            super(name, FieldType.FullText, sortable, noindex);
            this.weight = weight;
            this.nostem = nostem;
            this.phonetic = phonetic;
        }

        public TextField(FieldName name, double weight, boolean sortable, boolean nostem, boolean noindex, String phonetic) {
            super(name, FieldType.FullText, sortable, noindex);
            this.weight = weight;
            this.nostem = nostem;
            this.phonetic = phonetic;
        }

        @Override
        protected void serializeTypeArgs(List<String> args) {
            if (weight != 1.0) {
                args.add("WEIGHT");
                args.add(Double.toString(weight));
            }
            if (nostem) {
                args.add("NOSTEM");
            }
            if (phonetic != null) {
                args.add("PHONETIC");
                args.add(this.phonetic);
            }
        }

        @Override
        public String toString() {
            return "TextField{name='" + name + "', type=" + type + ", sortable=" + sortable + ", noindex=" + noindex
                    + ", weight=" + weight + ", nostem=" + nostem + ", phonetic='" + phonetic + "'}";
        }
    }

    public static class TagField extends Field {

        private final String separator;

        public TagField(String name) {
            this(name, null);
        }

        public TagField(String name, String separator) {
            this(name, separator, false);
        }

        public TagField(String name, boolean sortable) {
            this(name, null, sortable);
        }

        public TagField(String name, String separator, boolean sortable) {
            super(name, FieldType.Tag, sortable);
            this.separator = separator;
        }

        public TagField(FieldName name, String separator, boolean sortable) {
            super(name, FieldType.Tag, sortable, false);
            this.separator = separator;
        }

        @Override
        public void serializeTypeArgs(List<String> args) {
            if (separator != null) {
                args.add("SEPARATOR");
                args.add(separator);
            }
        }

        @Override
        public String toString() {
            return "TagField{name='" + name + "', type=" + type + ", sortable=" + sortable + ", noindex=" + noindex
                    + ", separator='" + separator + "'}";
        }
    }

    public final List<Field> fields;

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

    public Schema addSortableTagField(String name, String separator) {
        fields.add(new TagField(name, separator, true));
        return this;
    }

    public Schema addField(Field field) {
        fields.add(field);
        return this;
    }

    @Override public String toString() {
        return "Schema{fields=" + fields + "}";
    }
}
