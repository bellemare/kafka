package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class SimpleKTableRepartitionerProcessorSupplier<K, KR, VR> implements ProcessorSupplier<KR, Change<VR>> {

	public SimpleKTableRepartitionerProcessorSupplier() {

	}
	
	@Override
	public Processor<KR, Change<VR>> get() {
		return new UnbindChangeProcessor(); 
	}
	
	private class UnbindChangeProcessor extends AbstractProcessor<KR, Change<VR>>
	{

		@Override
		public void init(final ProcessorContext context) {
			super.init(context);
		}

		@Override
		public void process(KR key, Change<VR> change) {

            context().forward(key, change.newValue);
        }

		@Override
		public void close() {}
		
	}
}
