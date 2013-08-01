package com.ontology2.millipede.pop;

import com.ontology2.millipede.MultiFile;
import com.ontology2.millipede.sink.Sink;

public class Write<T> implements Millipede<T> {
    private final MultiFile<T> output;

    public Write(MultiFile<T> output) {
        this.output = output;
    }

    @Override
    public Sink<T> createSegment(int segmentNumber) throws Exception {
        return output.createSink(segmentNumber);
    }
}
