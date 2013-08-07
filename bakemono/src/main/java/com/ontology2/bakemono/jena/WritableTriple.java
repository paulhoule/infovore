package com.ontology2.bakemono.jena;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

//
// Makeshift way to serialize triples to the 
//
public class WritableTriple implements WritableComparable {
    private Triple innerTriple;
    private static final TripleComparator tc=new TripleComparator();

    public WritableTriple(Triple triple) {
        innerTriple=triple;
    }
    

    public WritableTriple() {
        this(null);
    }
 
    public WritableTriple(Node s, Node p, Node o) {
        innerTriple=new Triple(s,p,o);
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
    
    public boolean equals(Object o) {
        if (!(o instanceof WritableTriple))
            return false;
        
        WritableTriple that=(WritableTriple) o;
        return that.getTriple().equals(getTriple());
    }


    @Override
    public int compareTo(Object that) {
        return tc.compare(this.getTriple(), ((WritableTriple) that).getTriple());
    }

    @Override
    public int hashCode() {
        return getTriple().hashCode();
    }
}
