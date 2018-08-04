package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.streams.kstream.ValueJoiner;
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

public class LeftSideProcessorSupplier<KL, KR, VL, VR, V>
        implements ProcessorSupplier<CombinedKey<KR, KL>, PropagationWrapper<VL>>
{

    private final String topicName;
    private final KTableValueGetterSupplier<KR, VR> foreignValueGetterSupplier;
    private final ValueJoiner<VL, VR, V> joiner;

    //Right driven updates
    public LeftSideProcessorSupplier(String topicName,
                                     KTableValueGetterSupplier<KR, VR> foreignValueGetter,
                                     ValueJoiner<VL, VR, V> joiner)
    {
        this.topicName = topicName;
        this.joiner = joiner;
	    this.foreignValueGetterSupplier = foreignValueGetter;
    }


    @Override
    public Processor<CombinedKey<KR, KL>, PropagationWrapper<VL>> get()
    {

        return new AbstractProcessor<CombinedKey<KR, KL>, PropagationWrapper<VL>>()
        {

            private KeyValueStore<CombinedKey<KR, KL>, VL> store;
            private KTableValueGetter<KR, VR> foreignValues;

            @Override
            public void init(ProcessorContext context)
            {
                super.init(context);
                foreignValues = foreignValueGetterSupplier.get();
                foreignValues.init(context);
                store = (KeyValueStore<CombinedKey<KR,KL>, VL>) context.getStateStore(topicName);
            }

            @Override
            public void process(CombinedKey<KR,KL> key, PropagationWrapper<VL> value)
            {
                //We don't want to propagate a null due to a foreign-key change past this point.
                //Propagation of the updated value will occur in a different partition. State store needs deletion.
                if (!value.isPropagate()) {
                    store.delete(key);
//                    System.out.println("Deleted the value of "+key.toString()+" from store due to isPropagate");
                    return;
                }

//                System.out.println("LeftSide process (" + key.toString() +", " + value.toString() + ")");
                VL oldVal = store.get(key);
                store.put(key, value.getElem());
//                System.out.println("LeftSide LOADED TO SS: (" + key.toString() +", " + value.getElem().toString() + ")");
//                System.out.println("LeftSide Retry the get: (" + key.toString() +", " + store.get(key) + ")");


                V newValue = null;
                V oldValue = null;
                VR value2 = null;

                if (value.getElem() != null || oldVal != null) {
                    KR d = key.getForeignKey();
                    value2 = foreignValues.get(d);
//                    System.out.println("LeftSide - FK Get Result (" + d +", " + value2 + ")");
                }

                if (value.getElem() != null && value2 != null)
                    newValue = joiner.apply(value.getElem(), value2);

                if (oldVal != null && value2 != null)
                    oldValue = joiner.apply(oldVal, value2);

//                System.out.println("LeftSide - newValue = " + newValue + ", oldValue = " + oldValue);
                if(oldValue != null || newValue != null) {
                    KL realKey = key.getPrimaryKey();
                    //Use the offset of the original element, as it represents the original order of the now
                    //foreign-keyed data. This is used upon resolution of conflicts when everything is repartitioned back.
                    PropagationWrapper<V> newWrappedVal = new PropagationWrapper<>(newValue, true, value.getOffset());
                    PropagationWrapper<V> oldWrappedVal = new PropagationWrapper<>(oldValue, true, value.getOffset());

//                    System.out.println("LeftSide forward (" + realKey.toString() +", " + newWrappedVal.toString() + ")");
                    context().forward(realKey, new Change<>(newWrappedVal, oldWrappedVal));
                }
            }
        };
    }


    public KTableRangeValueGetterSupplier<CombinedKey<KR,KL>,VL> valueGetterSupplier() {
    	return new KTableSourceValueGetterSupplier<>(topicName);
    }
}
