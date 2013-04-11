package com.ontology2.millipede.counters;

import java.util.Map;

import com.google.common.collect.Maps;

public class SimpleCounter implements Counter {
	private final class SimpleCounterFace implements CounterFace {
		long count=0;

		@Override
		public void add(long amount) {
			count++;
		}
		
		long getCount() {
			return count;
		}
	}

	Map<Integer,SimpleCounterFace> faces=Maps.newHashMap();
	
	public CounterFace getFace(int binNumber) {
		if (!faces.containsKey(binNumber)) {
			faces.put(binNumber,_getFace(binNumber));
		}
		
		return faces.get(binNumber);
	}

	private SimpleCounterFace _getFace(int binNumber) {
		return new SimpleCounterFace();
	}

	@Override
	public long getCount() {
		long count=0;
		for(SimpleCounterFace f:faces.values()) {
			count+=f.getCount();
		}
		return count;
	}


}
