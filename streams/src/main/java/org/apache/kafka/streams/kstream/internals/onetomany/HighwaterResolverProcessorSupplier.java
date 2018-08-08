package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class HighwaterResolverProcessorSupplier<KL, V>
        implements ProcessorSupplier<KL, Change<PropagationWrapper<V>>>
{

    private final String stateStoreName;

    //Right driven updates
    public HighwaterResolverProcessorSupplier(String stateStoreName)
    {
        this.stateStoreName = stateStoreName;
    }

    @Override
    public Processor<KL, Change<PropagationWrapper<V>>> get()
    {
        return new AbstractProcessor<KL, Change<PropagationWrapper<V>>>()
        {
            private KeyValueStore<KL, Long> offsetHighWaterStore;

            @Override
            public void init(ProcessorContext context)
            {
                super.init(context);
                this.offsetHighWaterStore = (KeyValueStore<KL, Long>) context.getStateStore(stateStoreName);
            }

            @Override
            public void process(KL key, Change<PropagationWrapper<V>> value)
            {
                //highwater = X, value(offset = x+1, null)      => update & send
                //highwater = X, value(offset = x+1, non-null)  => update & send
                //highwater = X, value(offset = x, null)        => May occur if there is a system failure and we are restoring.
                //highwater = X, value(offset = x, non-null)    => May occur if there is a system failure and we are restoring.
                //highwater = X, value(offset = x-1, null)      => Do not send, it is an out-of-order, old update.
                //highwater = X, value(offset = x-1, non-null)  => Do not send, it is an out-of-order, old update.

                byte[] offsetHeader = context().headers().lastHeader("offset").value();
                Long offset = Serdes.Long().deserializer().deserialize("fakeTopic", offsetHeader);

                final Long highwater = offsetHighWaterStore.get(key);
                //if (null == highwater || value.newValue.getOffset() >= highwater ) {
                if (null == highwater || offset >= highwater ) {
                    // using greater-than to capture new highwater events.
                    // using equal as we want to resend in the case of a node failure.
                    offsetHighWaterStore.put(key, offset);
                    context().forward(key, value.newValue.getElem());
                } else if (offset == -1L ) {
                    //TODO - Is there a better way to forward from the right? Perhaps the topic metadata?
                    context().forward(key, value.newValue.getElem());
                }
            }
        };
    }
}
