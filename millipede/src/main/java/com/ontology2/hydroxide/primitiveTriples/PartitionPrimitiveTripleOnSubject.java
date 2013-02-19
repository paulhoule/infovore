package com.ontology2.hydroxide.primitiveTriples;

import org.apache.commons.codec.digest.DigestUtils;

import com.google.common.base.Splitter;
import com.ontology2.millipede.PartitionFunction;
import com.ontology2.millipede.Util;

public class PartitionPrimitiveTripleOnSubject implements PartitionFunction<PrimitiveTriple> {

	final int count;
	
	public PartitionPrimitiveTripleOnSubject(int count) {
		this.count = count;
	}
	
	@Override
	public int getPartitionCount() {
		return count;
	}

	@Override
	public int bin(PrimitiveTriple obj) {
		String target = computeTarget(obj);
	
		byte[] hashResult=DigestUtils.md5(target);
		long hashInt = Util.hashArrayToInt(hashResult);
		return (int) Math.abs(hashInt % count);
	}

	static String computeTarget(PrimitiveTriple obj) {
		return obj.subject;
	}

}
