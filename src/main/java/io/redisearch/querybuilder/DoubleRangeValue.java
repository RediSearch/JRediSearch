package io.redisearch.querybuilder;

/**
 * @author mnunberg on 2/23/18.
 */
public class DoubleRangeValue extends RangeValue {
    private double from;
    private double to;

    private static void appendNum(StringBuilder sb, double n, boolean inclusive) {
        if (!inclusive) {
            sb.append("(");
        }
        if (n == Double.NEGATIVE_INFINITY) {
            sb.append("-inf");
        } else if (n == Double.POSITIVE_INFINITY) {
            sb.append("inf");
        } else {
            sb.append(Double.toString(n));
        }
    }

    public DoubleRangeValue(double from, double to) {
        this.from = from;
        this.to = to;
    }

    @Override
    protected void appendFrom(StringBuilder sb, boolean inclusive) {
      appendNum(sb, from, inclusive);   
    }

    @Override
    protected void appendTo(StringBuilder sb, boolean inclusive) {
      appendNum(sb, to, inclusive);   
    }
}
