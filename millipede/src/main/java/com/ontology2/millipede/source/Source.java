package com.ontology2.millipede.source;

public interface Source<T> {
	public boolean hasMoreElements();
	public T nextElement() throws Exception;
}
