package com.ontology2.bakemono.jena;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.hp.hpl.jena.graph.Triple;

//
// Makeshift way to serialize triples to the 
//
public class WritableTriple implements Writable {
    private Triple innerTriple;

    public WritableTriple(Triple triple) {
        innerTriple=triple;
    }
    
 
    @Override
    public void write(DataOutput out) throws IOException {
        new WritableNode(innerTriple.getSubject()).write(out);
        new WritableNode(innerTriple.getPredicate()).write(out);
        new WritableNode(innerTriple.getObject()).write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        WritableNode s=takeNode(in);
        WritableNode p=takeNode(in);
        WritableNode o=takeNode(in);
        innerTriple = new Triple(s.getNode(),p.getNode(),o.getNode());
    }
    
    protected WritableNode takeNode(DataInput in) throws IOException {
        WritableNode n=new WritableNode();
        n.readFields(in);
        return n;
    }
    
    public Triple getTriple() {
        return innerTriple;
    }

}
