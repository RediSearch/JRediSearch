package io.redisearch.aggregation.reducers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by mnunberg on 2/22/18.
 *
 * This class is normally received via one of the subclasses or via Reducers
 */
public abstract class Reducer {
    private String alias = null;
    private String field = null;

    protected Reducer(String field) {
        this.field = field;
        this.alias = null;
    }

    protected Reducer() {
        this(null);
    }

    protected List<String> getOwnArgs() {
        if (field != null) {
            List<String> ret = new ArrayList<>();
            ret.add(field);
            return ret;
        } else {
            return Collections.emptyList();
        }
    }

    /**
     * @return The name of the reducer
     */
    public abstract String getName();

    public final String getAlias() {
        return alias;
    }

    public final Reducer setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public final Reducer as(String alias) {
        return setAlias(alias);
    }

    public final Reducer setAliasAsField() {
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("Cannot set to field name since no field exists");
        }
        return setAlias(field);
    }

    public final List<String> getArgs() {
        List<String> args = new ArrayList<>();
        List<String> ownArgs = getOwnArgs();
        args.add(Integer.toString(ownArgs.size()));
        args.addAll(ownArgs);
        return args;
    }
}
