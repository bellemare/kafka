package org.apache.kafka.streams.kstream.internals.graph;

import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import java.util.Collection;


public class CogroupedKTableNode<K, V> extends StreamsGraphNode {
    private final ProcessorSupplier cogroupProcessor;
    private final Collection<String> processorNames;
    private final Collection<String> sourceNodes;

    public CogroupedKTableNode(String nodeName,
                               boolean repartitionRequired,
                               ProcessorSupplier cogroupProcessor,
                               Collection<String> processorNames,
                               Collection<String> sourceNodes) {
        super(nodeName, repartitionRequired);
        this.cogroupProcessor = cogroupProcessor;
        this.processorNames = processorNames;
        this.sourceNodes = sourceNodes;
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        final String[] processorNamesArray = processorNames.toArray(new String[processorNames.size()]);
        System.out.println("BELLEMARE WAS HERE - processorNamesArray = ");
        for (String s: processorNamesArray) {
            System.out.println(s);
        }
        topologyBuilder.addProcessor(nodeName(), cogroupProcessor, processorNamesArray);
        topologyBuilder.copartitionSources(sourceNodes);
    }
}