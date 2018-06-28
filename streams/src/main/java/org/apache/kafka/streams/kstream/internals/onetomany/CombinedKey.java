package org.apache.kafka.streams.kstream.internals.onetomany;


import java.io.Serializable;

//TODO - How to serialize and deserialize this? I believe it will be in the materialized statestore, and we will need it to scan.
//TODO - Also, how does this fit in with the processors?
//TODO - Figure out where all of the combinedKeys are used, and then plan integration of this component accordingly.
public class CombinedKey<KL, KR> implements Serializable {
    private final KL leftKey;
    private final KR rightKey;

    public CombinedKey(final KL leftKey, final KR rightKey) {
        this.leftKey = leftKey;
        this.rightKey = rightKey;
    }

    public KL getLeftKey() {
        return this.leftKey;
    }

    public KR getRightKey() {
        return this.rightKey;
    }
}
