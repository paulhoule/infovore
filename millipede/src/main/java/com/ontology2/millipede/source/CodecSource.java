package com.ontology2.millipede.source;

import com.ontology2.millipede.Codec;

public class CodecSource<T> implements Source<T> {

	public CodecSource(Codec codec, LineSource source) {
		this.codec = codec;
		this.source = source;
	}

	private final Codec<T> codec;
	private final LineSource source;
	
	@Override
	public boolean hasMoreElements() {
		return source.hasMoreElements();
	}

	@Override
	public T nextElement() throws Exception {
		return codec.decode(source.nextElement());
	}

}
