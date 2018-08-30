package io.redisearch


import spock.lang.Specification

class SchemaSpec extends Specification {

    def setup() {
    }

    def "Schema builder "() {
        expect:
        new Schema().addGeoField("geofield").addNumericField("numeric")
                .addSortableNumericField("sortable")
                .addSortableTextField("sortabletext", 2)
                .addTextField("fieldname", 3) != null
    }


    def "Field serializeRedisArgs validate "() {
        List<String> args = []
        List<String> args2 = []

        when:
        Schema.Field field = new Schema.Field("MYFIELD", Schema.FieldType.FullText, true, true)
        field.serializeRedisArgs(args)

        then:
        args.size() == 4


        when:
        Schema.Field field2 = new Schema.Field("MYFIELD", Schema.FieldType.FullText, false, false)
        field2.serializeRedisArgs(args2)

        then:
        args2.size() == 2
    }

    def "TextField serializeRedisArgs validate "() {
        List<String> args = []
        List<String> args2 = []
        List<String> argsName = []
        when:
        Schema.TextField field = new Schema.TextField("MYFIELD", 1.0, true, true, true)
        field.serializeRedisArgs(args)

        then:
        args.size() == 5


        when:
        Schema.TextField field2 = new Schema.TextField("MYFIELD", 8, true, true)
        field2.serializeRedisArgs(args2)

        then:
        args2.size() == 6

        when:
        Schema.TextField fieldName = new Schema.TextField("MYFIELD")
        fieldName.serializeRedisArgs(argsName)

        then:
        argsName.size() == 2
    }


}


