package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

public enum ForeignKeyJoinInternalHeaderTypes {
    OFFSET("OFFSET"),
    PROPAGATE("PROPAGATE");
    private final String value;
    ForeignKeyJoinInternalHeaderTypes(final String s) { value = s; }
    public String toString() { return value; }
}