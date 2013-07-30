package com.ontology2.bakemono;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class TestParallelSuperEyeball3Mapper {

	PSE3Mapper pse3mapper;
	
	@Before
	public void setUp() {
		pse3mapper=new PSE3Mapper();
	};
	
	@Test
	public void testSimpleCase() {
	}

}
