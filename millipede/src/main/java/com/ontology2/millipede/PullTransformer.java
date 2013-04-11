package com.ontology2.millipede;

import com.google.common.base.Function;
import com.ontology2.millipede.sink.Sink;
import com.ontology2.millipede.source.Source;
import com.ontology2.millipede.source.TransformingSource;

public class PullTransformer<S,T> implements PullMultiSource<T> {

	public final PullMultiSource<S> innerSource;
	public final Function<S,T> transformingFunction;
	
	
	public PullTransformer(PullMultiSource<S> innerSource,
			Function<S, T> transformingFunction) {
		this.innerSource = innerSource;
		this.transformingFunction = transformingFunction;
	}

	@Override
	public int getPartitionCount() {
		return innerSource.getPartitionCount();
	}

	@Override
	public long pushBin(int binNumber, Sink<T> destination) throws Exception {
		return Plumbing.flow(createSource(binNumber), destination);
	}

	@Override
	public Source<T> createSource(int binNumber) throws Exception {
		return new TransformingSource(innerSource.createSource(binNumber),transformingFunction);
	}

}
