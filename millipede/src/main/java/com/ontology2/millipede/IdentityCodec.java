package com.ontology2.millipede;

public class IdentityCodec implements Codec<String> {

	@Override
	public java.lang.String encode(String obj) {
		return obj;
	}

	@Override
	public String decode(java.lang.String obj) {
		return obj;
	}

}
