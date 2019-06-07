package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class HighwaterResolverProcessorSupplier<KR, V>
        implements ProcessorSupplier<KR, Change<PropagationWrapper<V>>>
{

    private final String stateStoreName;

    //Right driven updates
    public HighwaterResolverProcessorSupplier(String stateStoreName)
    {
        this.stateStoreName = stateStoreName;
    }


    @Override
    public Processor<KR, Change<PropagationWrapper<V>>> get()
    {
        return new AbstractProcessor<KR, Change<PropagationWrapper<V>>>()
        {
            private KeyValueStore<KR, Long> offsetHighWaterStore;

            @Override
            public void init(ProcessorContext context)
            {
                super.init(context);
                this.offsetHighWaterStore = (KeyValueStore<KR, Long>) context.getStateStore(stateStoreName);
            }

            @Override
            public void process(KR key, Change<PropagationWrapper<V>> value)
            {
                //highwater = X, value(offset = x+1, null)      => update & send
                //highwater = X, value(offset = x+1, non-null)  => update & send
                //highwater = X, value(offset = x, null)        => May occur if there is a system failure and we are restoring.
                //highwater = X, value(offset = x, non-null)    => May occur if there is a system failure and we are restoring.
                //highwater = X, value(offset = x-1, null)      => Do not send, it is an out-of-order, old update.
                //highwater = X, value(offset = x-1, non-null)  => Do not send, it is an out-of-order, old update.

                final Long highwater = offsetHighWaterStore.get(key);
                if (null == highwater || value.newValue.getOffset() >= highwater ) {
                    // using greater-than to capture new highwater events.
                    // using equal as we want to resend in the case of a node failure.
                    offsetHighWaterStore.put(key, value.newValue.getOffset());
                    context().forward(key, value.newValue.getElem());
                } else if (value.newValue.getOffset() == -1 ) {
                    //TODO - Is there a better way to forward from the left? Perhaps the topic metadata?
                    context().forward(key, value.newValue.getElem());
                }
            }
        };
    }
}
