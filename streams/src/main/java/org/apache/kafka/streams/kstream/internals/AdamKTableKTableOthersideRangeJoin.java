//package org.apache.kafka.streams.kstream.internals;
//
//import org.apache.kafka.streams.kstream.ValueJoiner;
//import org.apache.kafka.streams.kstream.ValueMapper;
//import org.apache.kafka.streams.kstream.internals.Change;
//import org.apache.kafka.streams.kstream.internals.KTableImpl;
//import org.apache.kafka.streams.kstream.internals.KTableRangeValueGetterSupplier;
//import org.apache.kafka.streams.kstream.internals.KTableSourceValueGetterSupplier;
//import org.apache.kafka.streams.kstream.internals.KTableValueGetter;
//import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
//import org.apache.kafka.streams.processor.AbstractProcessor;
//import org.apache.kafka.streams.processor.Processor;
//import org.apache.kafka.streams.processor.ProcessorContext;
//import org.apache.kafka.streams.processor.ProcessorSupplier;
//import org.apache.kafka.streams.state.KeyValueStore;
//
//public class AdamKTableKTableOthersideRangeJoin<K0, V0, K, V, VO> implements ProcessorSupplier<K0, VO>
//{
//
//    private final KTableImpl<K0, ?, V0> table1;
//    private final KTableImpl<K, ?, VO> table2;
//    final KTableValueGetterSupplier<K0, V0> valueGetterSupplier1;
//    final KTableValueGetterSupplier<K, VO> valueGetterSupplier2;
//
//    final ValueMapper<K0, K> mapper;
//    //final ValueJoiner<V, VO, V0> joiner;
//
//    final ValueJoiner<? super V, ? super VO, ? extends V0> joiner;
//
//
//    public AdamKTableKTableOthersideRangeJoin(final KTableImpl<K0, ?, V0> table1,  //The left table.
//                                              final KTableImpl<K, ?, VO> table2,   //The altered-key right table.
//                                              final ValueMapper<K, K0> leftKeyExtractor,
//                                              final ValueMapper<K, K0> rightKeyExtractor, //TODO - Review the types, since I'm just using strings....
//                                              ValueMapper<K0, K> mapper,
//                                              ValueJoiner<V, VO, V0> joiner)
//    {
//        this.table1 = table1;
//        this.table2 = table2;
//        this.mapper = mapper;
//        this.joiner = joiner;
//        this.valueGetterSupplier1 = table1.valueGetterSupplier();
//        this.valueGetterSupplier2 = table2.valueGetterSupplier();
//    }
//
//    @Override
//    public Processor<K, Change<V0>> get() {
//        return new KTableKTableInnerJoin.KTableKTableJoinProcessor(valueGetterSupplier2.get());
//    }
//
//    @Override
//    public Processor<K0, Change<VO>> get()
//    {
//
//        return new AbstractProcessor<K0, Change<VO>>()
//        {
//
//            //Will receive new events like:
//            //  (1-5, <x,1>)
//            //  (2-6, <y,2>)
//            //  (2-5, <x,2>)
//            // Need to drive the output by:
//            // 1) Create the new key (which is just the right-side of the combinedKey)
//            // 2) Get the value from the left payload associated with the left-side of the combinedKey
//            // 3) Apply the joiner to the resultant values.
//
//            @Override
//            public void init(ProcessorContext context)
//            {
//                super.init(context);
//
//            }
//
//            @Override
//            public void process(K0 key, VO value)
//            {
//
//            }
//        };
//    }
//
////
////    public KTableRangeValueGetterSupplier<K0, VO> valueGetterSupplier() {
////        return new KTableSourceValueGetterSupplier<K0, VO>(topicName);
////    }
//}
