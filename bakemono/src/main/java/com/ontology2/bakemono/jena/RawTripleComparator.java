package com.ontology2.bakemono.jena;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RawTripleComparator extends WritableComparator {
    protected RawTripleComparator() {
        super(WritableTriple.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WritableTriple a$=(WritableTriple) a;
        WritableTriple b$=(WritableTriple) b;

        WritableNode a$Subject= subject(a$);
        WritableNode b$Subject= subject(b$);

        int value$Subject=a$Subject.compareTo(b$Subject);
        if (value$Subject!=0)
            return value$Subject;

        WritableNode a$Predicate= predicate(a$);
        WritableNode b$Predicate= predicate(b$);

        int value$Predicate=a$Predicate.compareTo(b$Predicate);
        if (value$Predicate!=0)
            return value$Predicate;

        WritableNode a$Object= object(a$);
        WritableNode b$Object= object(b$);
        return a$Object.compareTo(b$Object);

    }

    protected WritableNode subject(WritableTriple a$) {
        return new WritableNode(a$.getTriple().getSubject());
    }

    protected WritableNode predicate(WritableTriple a$) {
        return new WritableNode(a$.getTriple().getPredicate());
    }

    protected WritableNode object(WritableTriple a$) {
        return new WritableNode(a$.getTriple().getObject());
    }
}
