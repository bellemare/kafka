package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

public class SubscriptionResponseWrapper<FV> {
    private byte[] originalValueHash;
    private FV foreignValue;

    SubscriptionResponseWrapper(byte[] originalValueHash, FV foreignValue) {
        this.originalValueHash = originalValueHash;
        this.foreignValue = foreignValue;
    }

    public FV getForeignValue() {
        return foreignValue;
    }

    public byte[] getOriginalValueHash() {
        return originalValueHash;
    }
}
