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
        public final String name;
        public final FieldType type;
        public final boolean sortable;
        public final boolean noindex;

        public Field(String name, FieldType type, boolean sortable) {
        	this(name, type, sortable, false);
        }

        public Field(String name, FieldType type, boolean sortable, boolean noindex) {
            this.name = name;
            this.type = type;
            this.sortable = sortable;
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

    	private final double weight;
        private final boolean nostem;

        public TextField(String name) {
            this(name, 1.0);
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
        	super(name, FieldType.FullText, sortable, noindex);
            this.weight = weight;
            this.nostem = nostem;
        }

        @Override
        public void serializeRedisArgs(List<String> args) {
            args.add(name);
            args.add(type.str);
            if (weight != 1.0) {
                args.add("WEIGHT");
                args.add(Double.toString(weight));
            }
            if (nostem) {
                args.add("NOSTEM");
            }
            if (sortable) {
                args.add("SORTABLE");
            }
            if (noindex) {
                args.add("NOINDEX");
            }
        }
    }

    public static class TagField extends Field {
        private static final String DEFAULT_SEPARATOR = ",";
        
        private final String separator;

        public TagField(String name) {
        	this(name, DEFAULT_SEPARATOR);
        }

        public TagField(String name, String separator) {
        	this(name, separator, false);
        }

        public TagField(String name, boolean sortable) {
        	this(name, DEFAULT_SEPARATOR, sortable);
        }

        public TagField(String name, String separator, boolean sortable) {
        	super(name, FieldType.Tag, sortable);
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

    public final List<Field> fields;

    public Schema() {
        this.fields = new ArrayList<>();
    }

    /**
     * Add a text field to the schema with a given weight
     * @param name the field's name
     * @param weight its weight, a positive floating point number
     * @param sortable flag to handle if field will be sortable
     * @return the schema object
     */
    public Schema addTextField(String name, double weight, boolean sortable) {
        fields.add( new TextField( name, weight , sortable ) );
        return this;
    }

    /**
     * Add a text field to the schema with a given weight
     * @param name the field's name
     * @param weight its weight, a positive floating point number
     * @return the schema object
     */
    public Schema addTextField(String name, double weight) {
        return addTextField( name , weight , false);
    }

    /**
     * Add a text field that can be sorted on
     * @param name the field's name
     * @param weight its weight, a positive floating point number
     * @return the schema object
     */
    public Schema addSortableTextField(String name, double weight) {
        return addTextField( name , weight , true);
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
     * @param name the fields's name
     * @param sortable flag to handle if field will be sortable
     * @return the schema object
     */
    public Schema addNumericField(String name,boolean sortable) {
        fields.add( new Field(name, FieldType.Numeric, sortable));
        return this;
    }

    public Schema addNumericField(String name) {
        return addNumericField( name , false );
    }

    /* Add a numeric field that can be sorted on */
    public Schema addSortableNumericField(String name) {
        return addNumericField( name , true );
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
