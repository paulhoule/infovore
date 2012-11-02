package com.ontology2.millipede.source;

import java.util.Iterator;

public class Sources {
	public <T> Iterator<T> toIterator(final Source<T> s) {
		return new Iterator<T>() {

			@Override
			public boolean hasNext() {
				return s.hasMoreElements();
			}

			@Override
			public T next() {
				try {
					return s.nextElement();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
