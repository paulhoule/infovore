package com.ontology2.hydroxide;

import com.ontology2.millipede.*;

public class QuadCodec implements Codec<FreebaseQuad> {

	@Override
	public String encode(FreebaseQuad obj) {
		return obj.toString();
	}

	@Override
	public FreebaseQuad decode(String obj) {
		return FreebaseQuad.createFromLine(obj);
	}

}
