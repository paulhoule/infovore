package com.ontology2.millipede.sink;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.n3.turtle.TurtleEventHandler;

public class JenaTripleSink implements TurtleEventHandler {

    private final Sink<Triple> innerSink;
    private final boolean abortOnFail;
    private long count;

    public JenaTripleSink(Sink<Triple> innerSink) {
        this(innerSink,true);
    }

    public JenaTripleSink(Sink<Triple> innerSink,boolean abortOnFail) {
        this.innerSink=innerSink;
        this.abortOnFail=abortOnFail;
    }


    @Override
    public void triple(int line, int col, Triple triple) {
        try {
            innerSink.accept(triple);
        } catch (Exception e) {
            e.printStackTrace();
            if(abortOnFail) {
                System.exit(-1);
            }
        }
    }

    @Override
    public void endFormula(int arg0, int arg1) {
    }

    @Override
    public void prefix(int arg0, int arg1, String arg2, String arg3) {
    }

    @Override
    public void startFormula(int arg0, int arg1) {
    }

    public long getCount() {
        return count++;
    }

}
