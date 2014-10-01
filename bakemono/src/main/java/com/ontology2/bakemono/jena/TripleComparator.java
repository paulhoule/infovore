//package com.ontology2.bakemono.jena;
//
//import java.util.Comparator;
//
//import com.hp.hpl.jena.graph.Triple;
//
//public class TripleComparator implements Comparator<Triple> {
//
//    private final static NodeComparator nc=new NodeComparator();
//    @Override
//    public int compare(Triple o1, Triple o2) {
//        int val=nc.compare(o1.getSubject(),o2.getSubject());
//        if (val!=0)
//            return val;
//
//        val=nc.compare(o1.getPredicate(),o2.getPredicate());
//        if (val!=0)
//            return val;
//
//        return nc.compare(o1.getObject(),o2.getObject());
//    }
//
//}
