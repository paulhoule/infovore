package com.ontology2.bakemono.primitiveTriples;

import com.google.common.base.Function;

public class PrimitiveTripleTypeRewriter implements
        Function<PrimitiveTriple,PrimitiveTriple> {

    public PrimitiveTripleTypeRewriter(String oldType, String newType) {
        super();
        this.oldTemplate = "^^"+oldType;
        this.newType = newType;
    }
    
    private final String oldTemplate;
    private final String newType;
    
    @Override
    public PrimitiveTriple apply(PrimitiveTriple input) {
        String object=input.getObject();
        
        if(!object.endsWith(oldTemplate))
            return input;
        
        String newObject=object.substring(0,object.length()-oldTemplate.length()+2)+newType;
        return new PrimitiveTriple(input.getSubject(),input.getPredicate(),newObject);
    }

}
