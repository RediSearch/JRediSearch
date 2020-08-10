package io.redisearch;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by tgrall on 17/06/2020.
 */
public class SchemaTest {

    private final static String TITLE = "title";
    private final static String GENRE = "genre";
    private final static String VOTES = "votes";
    private final static String RATING = "rating";
    private final static String RELEASE_YEAR = "release_year";
    private final static String PLOT = "plot";

    @Test
    public void printSchemaTest() throws Exception {
        Schema sc =  new Schema()
                .addTextField(TITLE, 5.0)
                .addSortableTextField(PLOT, 1.0)
                .addSortableTagField(GENRE, ",")
                .addSortableNumericField(RELEASE_YEAR)
                .addSortableNumericField(RATING)
                .addSortableNumericField(VOTES);

        String schemaPrint = sc.toString();
        Assert.assertThat( schemaPrint, CoreMatchers.startsWith("Schema{fields=[TextField{name='title'"));
        Assert.assertThat( schemaPrint, CoreMatchers.containsString("{name='release_year', type=Numeric, sortable=true, noindex=false}"));
    }
}
