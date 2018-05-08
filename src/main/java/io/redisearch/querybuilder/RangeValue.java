package io.redisearch.querybuilder;

/**
 * @author mnunberg on 2/23/18.
 */
public class RangeValue extends Value {
    private double from;
    private double to;
    private boolean inclusiveMin = true;
    private boolean inclusiveMax = true;

    @Override
    public boolean isCombinable() {
        return false;
    }

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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        appendNum(sb, from, inclusiveMin);
        sb.append(" ");
        appendNum(sb, to, inclusiveMax);
        sb.append("]");
        return sb.toString();
    }

    public RangeValue(double from, double to) {
        this.from = from;
        this.to = to;
    }

    public RangeValue inclusiveMin(boolean val) {
        inclusiveMin = val;
        return this;
    }
    public RangeValue inclusiveMax(boolean val) {
        inclusiveMax = val;
        return this;
    }
}
