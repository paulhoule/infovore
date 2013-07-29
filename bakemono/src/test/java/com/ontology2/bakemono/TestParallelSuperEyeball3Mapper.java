package com.ontology2.bakemono;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class TestParallelSuperEyeball3Mapper {

	ParallelSuperEyeballMapper pse3mapper;
	
	@Before
	public void setUp() {
		pse3mapper=new ParallelSuperEyeballMapper();
	};
	
	@Test
	public void test() {
		assertNotNull(pse3mapper);
	}

}
