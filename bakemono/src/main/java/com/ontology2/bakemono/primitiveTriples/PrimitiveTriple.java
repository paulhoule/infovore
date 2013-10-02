package com.ontology2.bakemono.primitiveTriples;

import com.google.common.base.Predicate;

public class PrimitiveTriple {
    private final String subject;
    private final String predicate;
    private final String object;

    public PrimitiveTriple(String subject, String predicate, String object) {
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
    }

    public static Predicate<PrimitiveTriple> hasPredicate(final String thatPredicate) {
        return new Predicate<PrimitiveTriple>() {
            @Override
            public boolean apply(PrimitiveTriple arg0) {
                return arg0.getPredicate().equals(thatPredicate);
            }
        };
    }

    public static Predicate<PrimitiveTriple> objectMatchesPrefix(final String thatPrefix) {
        return new Predicate<PrimitiveTriple>() {
            @Override
            public boolean apply(PrimitiveTriple arg0) {
                return arg0.getObject().startsWith(thatPrefix);
            }
        };
    }
    
    public boolean equals(Object o) {
        if(o==null || !(o instanceof PrimitiveTriple))
            return false;
        
        PrimitiveTriple that = (PrimitiveTriple) o;
        return getSubject().equals(that.getSubject())
                && getObject().equals(that.getObject())
                && getPredicate().equals(that.getPredicate()); 
    }

    public String toString() {
        StringBuffer buff=new StringBuffer();
        buff.append(this.getSubject());
        buff.append("\t");
        buff.append(this.getPredicate());
        buff.append("\t");
        buff.append(this.getObject());
        buff.append(".\n");
        return buff.toString();
    }

    public String poPairAsString() {
        StringBuffer buff=new StringBuffer();
        buff.append(this.getPredicate());
        buff.append("\t");
        buff.append(this.getObject());
        buff.append(".");
        return buff.toString();		
    }

    public String getSubject() {
        return subject;
    }

    public String getPredicate() {
        return predicate;
    }

    public String getObject() {
        return object;
    }

}
