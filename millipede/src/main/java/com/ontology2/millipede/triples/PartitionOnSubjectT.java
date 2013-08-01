package com.ontology2.millipede.triples;

import java.nio.ByteBuffer;

import org.apache.commons.codec.digest.DigestUtils;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.ontology2.millipede.PartitionFunction;
import com.ontology2.millipede.Util;

public class PartitionOnSubjectT implements PartitionFunction<Triple> {

    final int count;

    public PartitionOnSubjectT(int count) {
        this.count = count;
    }

    @Override
    public int getPartitionCount() {
        // TODO Auto-generated method stub
        return count;
    }
    @Override

    public int bin(Triple t) {
        Node subject=t.getSubject();
        String value=subject.toString();
        byte[] hashResult=DigestUtils.md5("<"+value+">");
        long hashInt = Util.hashArrayToInt(hashResult);
        return (int) Math.abs(hashInt % count);
    }



}
