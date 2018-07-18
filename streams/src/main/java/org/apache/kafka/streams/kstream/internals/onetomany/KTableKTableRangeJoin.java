package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableImpl;
import org.apache.kafka.streams.kstream.internals.KTableMaterializedValueGetterSupplier;
import org.apache.kafka.streams.kstream.internals.KTableProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.KTableRangeValueGetter;
import org.apache.kafka.streams.kstream.internals.KTableRangeValueGetterSupplier;
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;


public class KTableKTableRangeJoin<KL, KR, VL, VR, V> implements KTableProcessorSupplier<KL, VL, V> {

	private final ValueJoiner<VL, VR, V> joiner;
	private final KTableRangeValueGetterSupplier<CombinedKey<KL,KR>,VR> right;
	private final String queryableName;
	private final StateStore ref;

    //Performs Left-driven updates (ie: new One, updates the Many).
    public KTableKTableRangeJoin(final KTableRangeValueGetterSupplier<CombinedKey<KL,KR>,VR> right,
                                 final ValueJoiner<VL, VR, V> joiner,
                                 final String queryableName,
                                 final StateStore ref){
    	this.right = right;
        this.joiner = joiner;
        this.queryableName = queryableName;
        this.ref = ref;
    }

	@Override
    public Processor<KL, Change<VL>> get() {
        return new KTableKTableJoinProcessor(right);
    }

    @Override
    public KTableValueGetterSupplier<KL, V> view() {
        return new KTableMaterializedValueGetterSupplier<>(queryableName);
    }

    public void enableSendingOldValues() {
        //TODO - Bellemare - evaluate if I need to propagate this upwards
    }

    private class KTableKTableJoinProcessor extends AbstractProcessor<KL, Change<VL>> {

		private final KTableRangeValueGetter<CombinedKey<KL,KR>,VR> rightValueGetter;
        private KeyValueStore<KR, V> store;

        public KTableKTableJoinProcessor(KTableRangeValueGetterSupplier<CombinedKey<KL,KR>,VR> right) {
            this.rightValueGetter = right.get();
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            rightValueGetter.init(context);
            if (queryableName != null) {
                store = (KeyValueStore<KR, V>) context.getStateStore(queryableName);
            }
        }

        /**
         * @throws StreamsException if key is null
         */
        @Override
        public void process(KL key, Change<VL> leftChange) {
            // the keys should never be null
            if (key == null)
                throw new StreamsException("Record key for KTable join operator should not be null.");

            //Wrap it in a combinedKey and let the serializer handle the prefixing.
            CombinedKey<KL,KR> prefixKey = new CombinedKey<>(key);

            //Flush the foreign state store, as we need all elements to be flushed for a proper range scan.
            ref.flush();
            final KeyValueIterator<CombinedKey<KL,KR>,VR> rightValues = rightValueGetter.prefixScan(prefixKey);

            while(rightValues.hasNext()){
                  KeyValue<CombinedKey<KL,KR>, VR> rightKeyValue = rightValues.next();
                  KR realKey = rightKeyValue.key.getRightKey();
                  VR value2 = rightKeyValue.value;
                  V newValue = null;
  				  V oldValue = null;

                  if (leftChange.oldValue != null) {
                	  oldValue = joiner.apply(leftChange.oldValue, value2);
                  }
                  
                  if (leftChange.newValue != null){
                      newValue = joiner.apply(leftChange.newValue, value2);
                  }

                  store.put(realKey, newValue);
                  context().forward(realKey, new Change<>(newValue, oldValue));
            }
        }
    }
}
