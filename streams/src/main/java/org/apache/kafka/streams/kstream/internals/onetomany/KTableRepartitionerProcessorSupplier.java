package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class KTableRepartitionerProcessorSupplier<KL, KR, VR> implements ProcessorSupplier<KR, Change<VR>> {

	private ValueMapper<VR, KL> mapper;

	public KTableRepartitionerProcessorSupplier(ValueMapper<VR,KL> extractor) {
		this.mapper = extractor;
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
			
			if(change.oldValue != null)
			{
				KL leftOldKey = mapper.apply(change.oldValue);
				CombinedKey<KL,KR> combinedOldKey = new CombinedKey<>(leftOldKey, key);
				if(change.newValue != null)
				{
					KL extractedNewLeftKey = mapper.apply(change.newValue);
					CombinedKey<KL, KR> combinedNewKey = new CombinedKey<>(extractedNewLeftKey, key);
					// This is a more tricky story 
					// I only want KR to be key of the new partition.

					// this wont work as we cant get a grab on the number of
					// partitions of the intermediate topic here
					// therefore the extractor/leftKeyExtractor has to extract the final K here already
					// so we can savely publish a delete and the update

					// we could skip the delete when we know we are in the same partition
					// and dealing with the same KR and end up in the same partition

					// IF they key equals, the intermediate key will equal which is used
					// to derive the partition
                    //TODO - How do I know they're equal?
					if(leftOldKey.equals(extractedNewLeftKey))
					{
					    //Same foreign key. Just propagate onwards.
						context().forward(combinedNewKey, new PropagationWrapper<>(change.newValue, true));
					}
					else  
					{
					    //Different Foreign Key - delete the old key value and propagate the new one.
						context().forward(combinedOldKey, new PropagationWrapper<>(change.newValue, false));
						context().forward(combinedNewKey, new PropagationWrapper<>(change.newValue, true));
					}
				}
				else
				{
					context().forward(combinedOldKey, new PropagationWrapper<>(null, true));
				}
			}
			else
			{
				if(change.newValue != null)
				{
					KL extractedLeftKeyValue = mapper.apply(change.newValue);
					CombinedKey<KL, KR> newCombinedKeyValue = new CombinedKey<>(extractedLeftKeyValue, key);
					context().forward(newCombinedKeyValue, new PropagationWrapper<>(change.newValue, true));
				}
				else
				{
					//Both null
				}
			}
		}

		@Override
		public void close() {}
		
	}
}
