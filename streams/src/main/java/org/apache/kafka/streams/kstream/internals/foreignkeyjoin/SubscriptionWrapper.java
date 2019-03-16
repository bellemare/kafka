package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

public class SubscriptionWrapper {
    private boolean propagate = false;
    private byte[] hash;

    public SubscriptionWrapper(byte[] hash, boolean propagate ) {
        this.propagate = propagate;
        this.hash = hash;
    }

    public boolean isPropagate() {
        return propagate;
    }

    public byte[] getHash() {
        return hash;
    }
}


/*
public class SubscriptionWrapper {
    public SubscriptionInstruction getInstruction() {
        return instruction;
    }

    private SubscriptionInstruction instruction;
    private byte[] hash;

    public SubscriptionWrapper(byte[] hash, SubscriptionInstruction instruction ) {
        this.instruction = instruction;
        this.hash = hash;
    }

    public byte[] getHash() {
        return hash;
    }
}
 */