package org.apache.kafka.streams.kstream.internals.onetomany;

public class PropagationWrapper<T> {

    private final T elem;
    private final boolean propagate;
    private final long offset;

    public PropagationWrapper(T elem, boolean propagate, long offset) {
        this.elem = elem;
        this.propagate = propagate;
        this.offset = offset;
    }

    public T getElem() {
        return elem;
    }

    public boolean isPropagate() {
        return propagate;
    }

    public long getOffset() { return offset;}

    @Override
    public String toString() {
        return "PropagationWrapper{" +
                "elem=" + elem +
                ", propagate=" + propagate +
                ", offset=" + offset +
                '}';
    }
}
