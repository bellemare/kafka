package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableMaterializedValueGetterSupplier;
import org.apache.kafka.streams.kstream.internals.KTableProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class PostJoinRepartitionerProcessorSupplier<KR, V0> implements KTableProcessorSupplier<KR, PropagationWrapper<V0>, V0> {
    private final String queryableName;
//    private final String queryableName;

	public PostJoinRepartitionerProcessorSupplier(String queryableName) {
        this.queryableName = queryableName;
	}

	@Override
	public Processor<KR, Change<PropagationWrapper<V0>>> get() {
		return new PrintableWrapperProcessor();
	}

//    @Override
//    public KTableValueGetterSupplier<KR, PropagationWrapper<V0>> view() {
//        return new KTableMaterializedValueGetterSupplier<>(queryableName);
//    }

	@Override
	public KTableValueGetterSupplier<KR, V0> view() {
		//TODO - Bellemare - handle the case where queryableName is not set... See Filter.
		return new KTableMaterializedValueGetterSupplier<>(queryableName);

	}

	public void enableSendingOldValues() {
		//Parent is a source
    }

	private class PrintableWrapperProcessor extends AbstractProcessor<KR, Change<PropagationWrapper<V0>>>
	{
        KeyValueStore<KR, V0> store;

		@Override
		public void init(final ProcessorContext context) {
			super.init(context);
            store = (KeyValueStore<KR, V0>) context.getStateStore(queryableName);

		}

		@Override
		public void process(KR key, Change<PropagationWrapper<V0>> value) {
			//Immediately discard anything that is not printable - these are the elements that have been deleted
			//that can cause race condition output.
			if (value.newValue.isPrintable()) {
			    V0 newVal = value.newValue.getElem();
                store.put(key, newVal);
				context().forward(key, new Change<>(newVal, value.oldValue));
			}
		}

		@Override
		public void close() {}
		
	}
}
