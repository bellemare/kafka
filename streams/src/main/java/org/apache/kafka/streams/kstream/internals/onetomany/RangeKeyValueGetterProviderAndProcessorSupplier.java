package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableRangeValueGetterSupplier;
import org.apache.kafka.streams.kstream.internals.KTableSourceValueGetterSupplier;
import org.apache.kafka.streams.kstream.internals.KTableValueGetter;
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class RangeKeyValueGetterProviderAndProcessorSupplier<K0, V0, K, V, VO> implements ProcessorSupplier<K0, VO>
{

    private final String topicName;
    private final ValueMapper<K0, K> leftKeyExtractor;
    private final ValueMapper<K0, K> rightKeyExtractor;
    private final KTableValueGetterSupplier<K, V> leftValueGetterSupplier;
    private final ValueJoiner<V, VO, V0> joiner;

    public RangeKeyValueGetterProviderAndProcessorSupplier(String topicName,
                                                           KTableValueGetterSupplier<K, V> leftValueGetter ,
                                                           ValueMapper<K0, K> leftKeyExtractor,
                                                           ValueMapper<K0, K> rightKeyExtractor,
                                                           ValueJoiner<V, VO, V0> joiner)
    {
        this.topicName = topicName;
        this.leftKeyExtractor = leftKeyExtractor;
        this.rightKeyExtractor = rightKeyExtractor;
        this.joiner = joiner;
	    this.leftValueGetterSupplier = leftValueGetter;
    }


    @Override
    public Processor<K0, VO> get()
    {

        return new AbstractProcessor<K0, VO>()
        {

            KeyValueStore<K0, VO> store;
            KTableValueGetter<K, V> leftValues;

            @Override
            public void init(ProcessorContext context)
            {
                super.init(context);
                leftValues = leftValueGetterSupplier.get();
                leftValues.init(context);
                store = (KeyValueStore<K0, VO>) context.getStateStore(topicName);
            }

            @Override
            public void process(K0 key, VO value)
            {
                VO oldVal = store.get(key);
                store.put(key, value);

                V0 newValue = null;
                V0 oldValue = null;
                V value2 = null;

                if (value != null || oldVal != null) {
                    K d = leftKeyExtractor.apply(key);
                    value2 = leftValues.get(d);
                }

                if (value != null && value2 != null)
                    newValue = joiner.apply(value2, value);

                if (oldVal != null && value2 != null)
                    oldValue = joiner.apply(value2, oldVal);

                //TODO - Bellemare - Am I using the right generic types in this class?
                if(oldValue != null || newValue != null) {
                    K realKey = rightKeyExtractor.apply(key);
                    context().forward(realKey, new Change<>(newValue, oldValue));
                }
            }
        };
    }

    
    public KTableRangeValueGetterSupplier<K0, VO> valueGetterSupplier() {
    	return new KTableSourceValueGetterSupplier<K0, VO>(topicName);
    }
}
