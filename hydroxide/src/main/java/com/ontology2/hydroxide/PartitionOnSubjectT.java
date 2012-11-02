package com.ontology2.hydroxide;

import java.nio.ByteBuffer;

import org.apache.commons.codec.digest.DigestUtils;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.ontology2.millipede.PartitionFunction;

import static com.ontology2.basekb.StatelessIdFunctions.*;
import static com.ontology2.basekb.jena.StatelessJenaFunctions.*;

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
