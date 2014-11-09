package com.ontology2.bakemono.primitiveTriples;

import com.google.common.base.Function;


public class PrimitiveTripleReverser implements Function<PrimitiveTriple,PrimitiveTriple> {

    final private String from;
    final private String to;

    public PrimitiveTripleReverser(String from, String to) {
        this.from = from;
        this.to = to;
    }

    @Override
    public PrimitiveTriple apply(PrimitiveTriple obj) {
        if(from.equals(obj.getPredicate())) {
            return new PrimitiveTriple(
                    obj.getObject(),
                    to,
                    obj.getSubject()	
                    );
        } else {
            return obj;
        }	
    }


}
