package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

public class SubscriptionWrapper {
    private boolean propagate;
    private long[] hash;

    public SubscriptionWrapper(long[] hash, boolean propagate ) {
        this.propagate = propagate;
        this.hash = hash;
    }

    public boolean isPropagate() {
        return propagate;
    }

    public long[] getHash() {
        return hash;
    }
}