package com.ontology2.millipede.sink;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

public class ListSink<T> extends EmptyReportSink<T> {
	
	List<T> innerList=Lists.newArrayList();

	@Override
	public void accept(T obj) throws Exception {
		innerList.add(obj);
	}

	public List<T> getContent() {
		return Collections.unmodifiableList(innerList);
	};
}
