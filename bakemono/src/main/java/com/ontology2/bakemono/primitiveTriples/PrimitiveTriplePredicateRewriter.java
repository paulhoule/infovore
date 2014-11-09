package com.ontology2.bakemono.primitiveTriples;

import com.google.common.base.Function;

public class PrimitiveTriplePredicateRewriter implements Function<PrimitiveTriple,PrimitiveTriple> {
    private final String from;
    private final String to;

    public PrimitiveTriplePredicateRewriter(String from, String to) {
        this.from = from;
        this.to = to;
    }

    public PrimitiveTriple apply(PrimitiveTriple obj)  {
        if(from.equals(obj.getPredicate())) {
            return new PrimitiveTriple(
                    obj.getSubject(),
                    to,
                    obj.getObject()	
                    );
        } else {
            return obj;
        }	
    }
}
