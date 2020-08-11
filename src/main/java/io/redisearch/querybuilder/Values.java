package io.redisearch.querybuilder;

import java.util.StringJoiner;

/**
 * Created by mnunberg on 2/23/18.
 */
public class Values {
    private Values() {
    }

    private abstract static class ScalableValue extends Value {
        @Override
        public boolean isCombinable() {
            return true;
        }
    }

    public static Value value(String s) {
        return new ScalableValue() {
            @Override
            public String toString() {
                return s;
            }
        };
    }

    public static RangeValue between(double from, double to) {
        return new DoubleRangeValue(from, to);
    }

    public static RangeValue between(int from, int to) {
        return new LongRangeValue(from, to);
    }

    public static RangeValue eq(double d) {
        return new DoubleRangeValue(d, d);
    }

    public static RangeValue eq(int i) {
      return new LongRangeValue(i, i);
    }

    public static RangeValue lt(double d) {
        return new DoubleRangeValue(Double.NEGATIVE_INFINITY, d).inclusiveMax(false);
    }

    public static RangeValue lt(int d) {
      return new LongRangeValue(Long.MIN_VALUE, d).inclusiveMax(false);
    }

    public static RangeValue gt(double d) {
        return new DoubleRangeValue(d, Double.POSITIVE_INFINITY).inclusiveMin(false);
    }
    
    public static RangeValue gt(int d) {
      return new LongRangeValue(d, Long.MAX_VALUE).inclusiveMin(false);
    }

    public static RangeValue le(double d) {
        return lt(d).inclusiveMax(true);
    }
    
    public static RangeValue le(int d) {
      return lt(d).inclusiveMax(true);
    }

    public static RangeValue ge(double d) {
        return gt(d).inclusiveMin(true);
    }
    
    public static RangeValue ge(int d) {
      return gt(d).inclusiveMin(true);
    }


    public static Value tags(String ...tags) {
        if (tags.length == 0) {
            throw new IllegalArgumentException("Must have at least one tag");
        }
        StringJoiner sj = new StringJoiner(" | ");
        for (String s : tags) {
            sj.add(s);
        }
        return new Value() {
            @Override
            public String toString() {
                return "{" + sj.toString() + "}";
            }
        };
    }
}
