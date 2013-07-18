package com.ontology2.bakemono;

import static org.junit.Assert.*;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.junit.Test;

public class MainTest {

	@Test
	public void whatIsTheDefaultReducer() {
		JobConf conf = new JobConf(Main.class);
		assertEquals(IdentityReducer.class,conf.getReducerClass());
	}

}
