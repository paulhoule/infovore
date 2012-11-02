package com.ontology2.millipede;

public class NullCodec implements Codec {

	@Override
	public String encode(Object obj) {
		return null;
	}

	@Override
	public Object decode(String obj) {
		return null;
	}

}
