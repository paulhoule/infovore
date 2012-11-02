package com.ontology2.hydroxide.turtleThree;

import static org.junit.Assert.*;

import org.junit.Test;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDateTime;
import com.hp.hpl.jena.graph.Node;

import static com.ontology2.hydroxide.turtleThree.FbDateConversion.*;

public class TestDateParsing {

	@Test
	public void testHourMinuteTime() {
		Node n=convertFreebaseDate(" 23:17");
		assertTrue(isValidDate(n));
		
		assertEquals(n.getLiteralDatatype(),XSDDatatype.XSDtime);
		XSDDateTime time=(XSDDateTime) n.getLiteralValue();
		assertEquals(23,time.getHours());
		assertEquals(17,time.getMinutes());
	}
	
	@Test
	public void testHourTime() {
		Node n=convertFreebaseDate(" 15");
		assertTrue(isValidDate(n));
		
		assertEquals(n.getLiteralDatatype(),XSDDatatype.XSDtime);
		XSDDateTime time=(XSDDateTime) n.getLiteralValue();
		assertEquals(15,time.getHours());
	}
	
	@Test
	public void testFulldateTime() {
		Node n=convertFreebaseDate("1972-04-24 16:05:19");
		assertTrue(isValidDate(n));
		
		assertEquals(n.getLiteralDatatype(),XSDDatatype.XSDdateTime);
		XSDDateTime time=(XSDDateTime) n.getLiteralValue();
		assertEquals(1972,time.getYears());
		assertEquals(4,time.getMonths());
		assertEquals(24,time.getDays());
		assertEquals(16,time.getHours());
		assertEquals(05,time.getMinutes());
		assertEquals(19,time.getSeconds(),0.01);
	}
	
	@Test
	public void testJustDate() {
		Node n=convertFreebaseDate("1972-04-24");
		assertTrue(isValidDate(n));
		
		assertEquals(n.getLiteralDatatype(),XSDDatatype.XSDdate);
		XSDDateTime time=(XSDDateTime) n.getLiteralValue();
		assertEquals(1972,time.getYears());
		assertEquals(4,time.getMonths());
		assertEquals(24,time.getDays());
	}
	
	@Test
	public void testYearMonth() {
		Node n=convertFreebaseDate("2012-04");
		assertTrue(isValidDate(n));
		
		assertEquals(n.getLiteralDatatype(),XSDDatatype.XSDdate);
		XSDDateTime time=(XSDDateTime) n.getLiteralValue();
		assertEquals(2012,time.getYears());
		assertEquals(4,time.getMonths());
	}

}
