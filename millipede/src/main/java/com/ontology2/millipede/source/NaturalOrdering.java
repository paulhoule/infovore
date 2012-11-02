package com.ontology2.millipede.source;

import java.util.Comparator;

public class NaturalOrdering<T extends Comparable<T>> implements Comparator<T> {

	@Override
	public int compare(T o1, T o2) {
		return o1.compareTo(o2);
	}

}
