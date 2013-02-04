package com.ontology2.millipede;

import java.nio.ByteBuffer;

public class Util {
	public static long hashArrayToInt(byte[] hashResult) {
		return ByteBuffer.wrap(hashResult).getLong();
	}
}
