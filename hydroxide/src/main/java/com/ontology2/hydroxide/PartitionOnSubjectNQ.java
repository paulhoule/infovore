package com.ontology2.hydroxide;

import static com.ontology2.basekb.jena.StatelessJenaFunctions.*;

import java.nio.ByteBuffer;

import org.apache.commons.codec.digest.DigestUtils;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Quad;
import com.ontology2.millipede.PartitionFunction;

public class PartitionOnSubjectNQ implements PartitionFunction<Quad> {

	final int count;
	
	public PartitionOnSubjectNQ(int count) {
		this.count = count;
	}

	@Override
	public int getPartitionCount() {
		return count;
	}
	
	@Override
	public int bin(Quad t) {
		Node subject=t.getSubject();
		String target=toFb(subject);
		target = (null==target)? subject.toString() : target;
		   
		byte[] hashResult=DigestUtils.md5(target);
		long hashInt = hashArrayToInt(hashResult);
		return (int) Math.abs(hashInt % count);
	}

	public static long hashArrayToInt(byte[] hashResult) {
		return ByteBuffer.wrap(hashResult).getLong();
	}
}
