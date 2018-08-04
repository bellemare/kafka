package org.apache.kafka.streams.kstream.internals.onetomany;

public class CombinedKey<KF, KP> {
    private final KF foreignKey;
    private KP primaryKey = null;

    public CombinedKey(final KF foreignKey, final KP primaryKey) {
        this.foreignKey = foreignKey;
        this.primaryKey = primaryKey;
    }

    public CombinedKey(final KF leftKey) {
        this.foreignKey = leftKey;
    }

    public KF getForeignKey() {
        return this.foreignKey;
    }

    public KP getPrimaryKey() {
        return this.primaryKey;
    }

    public boolean equals(KF leftKey, KP rightKey) {
        return this.foreignKey.equals(leftKey) && this.primaryKey.equals(rightKey);
    }

    @Override
    public String toString() {
        return "CombinedKey{" +
                "foreignKey=" + foreignKey +
                ", primaryKey=" + primaryKey +
                '}';
    }
}
