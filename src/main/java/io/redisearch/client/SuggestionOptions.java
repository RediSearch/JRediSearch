package io.redisearch.client;

import java.util.Optional;

public class SuggestionOptions {

    private With with;
    private boolean fuzzy;
    private int max;

    private SuggestionOptions(Builder builder) {
        this.with = builder.with;
        this.fuzzy = builder.fuzzy;
        this.max = builder.max;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Optional<With> getWith() {
        return Optional.ofNullable(with);
    }

    public boolean isFuzzy() {
        return fuzzy;
    }

    public int getMax() {
        return max;
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public enum With {
        PAYLOAD(Client.WITHPAYLOADS_FLAG),
        SCORES(Client.WITHSCORES_FLAG),
        PAYLOAD_AND_SCORES(Client.WITHPAYLOADS_FLAG, Client.WITHSCORES_FLAG);
        final String[] flags;

        With(String... flags) {
            this.flags = flags;
        }

        public String[] getFlags() {
            return this.flags;
        }
    }

    public static final class Builder {
        private With with;
        private Boolean fuzzy;
        private Integer max;

        public Builder() {
        }

        private Builder(SuggestionOptions options) {
            this.with = options.with;
            this.fuzzy = options.fuzzy;
            this.max = options.getMax();
        }

        public Builder with(SuggestionOptions.With with) {
            this.with = with;
            return this;
        }

        public Builder fuzzy() {
            this.fuzzy = true;
            return this;
        }

        public Builder max(int max) {
            this.max = max;
            return this;
        }

        public SuggestionOptions build() {
            if (this.fuzzy == null) {
                this.fuzzy = false;
            }
            if (this.max == null) {
                this.max = 5;
            }

            return new SuggestionOptions(this);
        }
    }

}
