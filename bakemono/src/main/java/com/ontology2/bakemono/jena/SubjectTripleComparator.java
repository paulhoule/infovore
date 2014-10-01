package com.ontology2.bakemono.jena;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SubjectTripleComparator extends RawTripleComparator {
    protected SubjectTripleComparator() {
        super();
    }

    protected SubjectTripleComparator(boolean createInstances) {
        super(createInstances);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WritableTriple a$=(WritableTriple) a;
        WritableTriple b$=(WritableTriple) b;

        WritableNode a$Subject= subject(a$);
        WritableNode b$Subject= subject(b$);

        return a$Subject.compareTo(b$Subject);
    }
}
