package com.ontology2.millipede.pop;

import com.hp.hpl.jena.rdf.model.Model;
import com.ontology2.millipede.counters.CounterFace;
import com.ontology2.millipede.counters.SimpleCounter;
import com.ontology2.millipede.sink.EmptyReportSink;
import com.ontology2.millipede.sink.Sink;

public class Uniq<T> implements Millipede<T> {

    private final Millipede<T> output;
    private final SimpleCounter duplicates;

    public Uniq(Millipede<T> output) {
        this.output = output;
        duplicates=new SimpleCounter();
    }

    @Override
    public Sink<T> createSegment(final int segmentNumber) throws Exception {
        final Sink outputSink=output.createSegment(segmentNumber);
        final CounterFace duplicatesFace=duplicates.getFace(segmentNumber);
        return new EmptyReportSink<T>() {
            T last=null;

            @Override
            public void accept(T that) throws Exception {
                if (last == null || !that.equals(last)) {
                    outputSink.accept(that);
                } else {
                    duplicatesFace.add(1);
                }
                last=that;
            }

            @Override
            public Model close() throws Exception {
                outputSink.close();
                return super.close();
            }
        };
    }

    public long getDuplicatesCount() {
        return duplicates.getCount();
    }
}
