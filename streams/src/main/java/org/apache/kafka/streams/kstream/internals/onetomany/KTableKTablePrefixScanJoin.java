package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableRangeValueGetter;
import org.apache.kafka.streams.kstream.internals.KTableRangeValueGetterSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;


public class KTableKTablePrefixScanJoin<KL, KR, VL, VR, V> implements ProcessorSupplier<KR, Change<VR>> {

	private final ValueJoiner<VL, VR, V> joiner;
	private final KTableRangeValueGetterSupplier<CombinedKey<KR, KL>, VL> primary;
	private final StateStore ref;

    public KTableKTablePrefixScanJoin(KTableRangeValueGetterSupplier<CombinedKey<KR, KL>,VL> primary,
                                      ValueJoiner<VL, VR, V> joiner,
                                      StateStore ref){

    	this.primary = primary;
        this.joiner = joiner;
        this.ref = ref;
    }

	@Override
    public Processor<KR, Change<VR>> get() {
        return new KTableKTableJoinProcessor(primary);
    }
	

    private class KTableKTableJoinProcessor extends AbstractProcessor<KR, Change<VR>> {

		private final KTableRangeValueGetter<CombinedKey<KR,KL>,VL> leftValueGetter;

        public KTableKTableJoinProcessor(KTableRangeValueGetterSupplier<CombinedKey<KR,KL>,VL> left) {
            this.leftValueGetter = left.get();
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            leftValueGetter.init(context);
        }

        /**
         * @throws StreamsException if key is null
         */
        @Override
        public void process(KR key, Change<VR> change) {
//            System.out.println("PrefixScan process (" + key.toString() +", " + change.toString() + ")");

            // the keys should never be null
            if (key == null)
                throw new StreamsException("Record key for KTable join operator should not be null.");

            //Wrap it in a combinedKey and let the serializer handle the prefixing.
            CombinedKey<KR,KL> prefixKey = new CombinedKey<>(key);

            //Flush the foreign state store, as we need all elements to be flushed for a proper range scan.
            ref.flush();
            final KeyValueIterator<CombinedKey<KR,KL>,VL> rightValues = leftValueGetter.prefixScan(prefixKey);

            boolean results = false;

            while(rightValues.hasNext()){

                KeyValue<CombinedKey<KR,KL>, VL> rightKeyValue = rightValues.next();

                results = true;
//                System.out.println("PrefixScan scan-result (" + rightKeyValue.key.toString() +", " + rightKeyValue.value.toString() + ")");


                KL realKey = rightKeyValue.key.getPrimaryKey();
                VL value2 = rightKeyValue.value;
                V newValue = null;
                V oldValue = null;

                if (change.oldValue != null) {
                    oldValue = joiner.apply(value2, change.oldValue);
                }

                if (change.newValue != null){
                    newValue = joiner.apply(value2, change.newValue);
                }
                //TODO - Possibly rework this so that it's a different wrapper. We don't need the printable part anymore, but it's annoying to have to create another nearly-the-same class.
                //Using -1 because we will not have race conditions from this side of the join to disambiguate with source offset.
                PropagationWrapper<V> newWrappedVal = new PropagationWrapper<>(newValue, true, -1);
                PropagationWrapper<V> oldWrappedVal = new PropagationWrapper<>(oldValue, true, -1);

//                  System.out.println("PrefixScan forward (" + realKey.toString() +", " + newWrappedVal.toString() + ")");
                context().forward(realKey, new Change<>(newWrappedVal, oldWrappedVal));
            }
            if (!results) {
//                System.out.println("PrefixScan result: Empty!");
            }
        }
    }
}
