package io.redisearch;

import java.util.Arrays;
import java.util.Objects;

public class Suggestion {

    private String string;
    private double score;
    private byte[] payload;


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

    public byte[] getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Suggestion that = (Suggestion) o;
        return Double.compare(that.getScore(), getScore()) == 0 &&
                Objects.equals(getString(), that.getString()) &&
                Arrays.equals(getPayload(), that.getPayload());
    }

    @Override
    public int hashCode() {

        int result = Objects.hash(getString(), getScore());
        result = 31 * result + Arrays.hashCode(getPayload());
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Suggestion{");
        sb.append("string='").append(string).append('\'');
        sb.append(", score=").append(score);
        sb.append(", payload=").append(Arrays.toString(payload));
        sb.append('}');
        return sb.toString();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public static final class Builder {
        private String string;
        private double score = 1.0;
        private byte[] payload;

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

        public Builder payload(byte[] payload) {
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
