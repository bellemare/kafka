package org.apache.kafka.streams.kstream.internals.onetomany;

public class PropagationWrapper<T> {

    private final T elem;
    private final boolean printable;

    public PropagationWrapper(T elem, boolean printable) {
        this.elem = elem;
        this.printable = printable;
    }

    public T getElem() {
        return elem;
    }

    public boolean isPrintable() {
        return printable;
    }


}
