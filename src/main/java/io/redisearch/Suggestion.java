package io.redisearch;

import java.util.Objects;

public final class Suggestion {

    private String string;
    private double score;
    private String payload;


    private Suggestion(Builder b) {
        this.string = b.string;
        this.score = b.score;
        this.payload = b.payload;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getString() {
        return string;
    }

    public double getScore() {
        return score;
    }

    public String getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Suggestion that = (Suggestion) o;
        return Double.compare(that.getScore(), getScore()) == 0 &&
                Objects.equals(getString(), that.getString()) &&
                Objects.equals(getPayload(), that.getPayload());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getString(), getScore(), getPayload());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Suggestion{");
        sb.append("string='").append(string).append('\'');
        sb.append(", score=").append(score);
        sb.append(", payload='").append(payload).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public static final class Builder {
        private String string;
        private double score = 1.0;
        private String payload;

        public Builder() {
        }

        private Builder(Suggestion suggestion) {
            this.string = suggestion.getString();
            this.score = suggestion.score;
            this.payload = suggestion.getPayload();
        }

        public Builder str(String str) {
            this.string = str;
            return this;
        }


        public Builder score(double score) {
            this.score = score;
            return this;
        }

        public Builder payload(String payload) {
            this.payload = payload;
            return this;
        }

        public Suggestion build() {
            String missing = "";
            if (this.string == null) {
                missing += " string";
            }
            if (this.score < 0.0 || this.score > 1.0) {
                missing += " score not within range";
            }

            if (!missing.isEmpty()) {
                throw new IllegalStateException("Missing required fields:" + missing);
            }

            return new Suggestion(this);
        }
    }

}
