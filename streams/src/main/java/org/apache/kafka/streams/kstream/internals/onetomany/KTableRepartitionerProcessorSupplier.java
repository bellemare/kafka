package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class KTableRepartitionerProcessorSupplier<KL, KR, VL> implements ProcessorSupplier<KL, Change<VL>> {

	private final ValueMapper<VL, KR> mapper;

	public KTableRepartitionerProcessorSupplier(ValueMapper<VL,KR> extractor) {
		this.mapper = extractor;
	}
	
	@Override
	public Processor<KL, Change<VL>> get() {
		return new UnbindChangeProcessor(); 
	}
	
	private class UnbindChangeProcessor extends AbstractProcessor<KL, Change<VL>>
	{

		@Override
		public void init(final ProcessorContext context) {
			super.init(context);
		}

		@Override
		public void process(KL key, Change<VL> change) {
			
			if(change.oldValue != null)
			{
				KR oldForeignKey = mapper.apply(change.oldValue);
				CombinedKey<KR,KL> combinedOldKey = new CombinedKey<>(oldForeignKey, key);
				if(change.newValue != null)
				{
					KR extractedNewForeignKey = mapper.apply(change.newValue);
					CombinedKey<KR, KL> combinedNewKey = new CombinedKey<>(extractedNewForeignKey, key);

					//TODO - Requires equal to be defined. If not defined, should still resolve to same in the else-statement.
					if(oldForeignKey.equals(extractedNewForeignKey))
					{
					    //Same foreign key. Just propagate onwards.
						context().forward(combinedNewKey, new PropagationWrapper<>(change.newValue, true, context().offset()));
					}
					else  
					{
					    //Different Foreign Key - delete the old key value and propagate the new one.
                        //Note that we indicate that we don't want to propagate the delete to the join output. It is set to false.
                        //This will be used by a downstream processor to delete it from the local state store, but not propagate it
                        //as a full delete. This avoids a race condition in the resolution of the output.
						context().forward(combinedOldKey, new PropagationWrapper<>(change.newValue, false, context().offset()));
						context().forward(combinedNewKey, new PropagationWrapper<>(change.newValue, true, context().offset()));
					}
				}
				else
				{
					context().forward(combinedOldKey, new PropagationWrapper<>(null, true, context().offset()));
				}
			}
			else
			{
				if(change.newValue != null)
				{
					KR extractedForeignKeyValue = mapper.apply(change.newValue);
					CombinedKey<KR, KL> newCombinedKeyValue = new CombinedKey<>(extractedForeignKeyValue, key);
					context().forward(newCombinedKeyValue, new PropagationWrapper<>(change.newValue, true, context().offset()));
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
