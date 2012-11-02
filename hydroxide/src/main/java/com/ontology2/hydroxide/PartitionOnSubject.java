package com.ontology2.hydroxide;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.apache.commons.codec.digest.DigestUtils;

import com.ontology2.basekb.StatelessIdFunctions;
import com.ontology2.millipede.PartitionFunction;


public class PartitionOnSubject implements PartitionFunction<FreebaseQuad> {

	private int count;

	public PartitionOnSubject(int count) {
		this.count=count;
	}
	
	@Override
	public int getPartitionCount() {
		// TODO Auto-generated method stub
		return count;
	}

	@Override
	public int bin(FreebaseQuad obj) {
		return StatelessIdFunctions.hashRawMid(obj.getSubject(), count);
	}

}
