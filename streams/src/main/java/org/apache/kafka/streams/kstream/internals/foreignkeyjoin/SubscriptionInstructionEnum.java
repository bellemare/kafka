package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

enum SubscriptionInstruction {
    DELETE_AND_PROPAGATE, DELETE_NO_PROPAGATE, PROPAGATE
}
