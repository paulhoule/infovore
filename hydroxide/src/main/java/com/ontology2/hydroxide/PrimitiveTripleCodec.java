package com.ontology2.hydroxide;

import com.ontology2.millipede.Codec;

public class PrimitiveTripleCodec implements Codec<PrimitiveTriple> {

	@Override
	public String encode(PrimitiveTriple obj) {
		StringBuilder output=new StringBuilder();
		output.append(obj.subject);
		output.append("\t");
		output.append(obj.predicate);
		output.append("\t");
		output.append(obj.object);
		output.append(".");
		return output.toString();
	}

	@Override
	public PrimitiveTriple decode(String obj) {
		throw new RuntimeException("Decode not implemented for primitive triple");
	}

}
