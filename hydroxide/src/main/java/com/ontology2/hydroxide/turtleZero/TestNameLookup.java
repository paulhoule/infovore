package com.ontology2.hydroxide.turtleZero;

public class TestNameLookup {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		TurtleZero t0=new TurtleZero();
		System.out.println(t0.lookup("/authority/netflix"));
	}

}
