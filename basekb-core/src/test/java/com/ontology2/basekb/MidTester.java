package com.ontology2.basekb;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import static com.ontology2.basekb.StatelessIdFunctions.*;

public class MidTester {
		@Test
		public void toIntTest1() {
			long value=midToLong("/m/01");
			assertEquals(1,value);
		}
		
		@Test
		public void toIntTest2() {
			long value=midToLong("/m/0b");
			assertEquals(10,value);
		}
		
		@Test
		public void toIntTest3() {
			long value=midToLong("/m/0b0");
			assertEquals(320,value);
		}
		
		@Test
		public void midToGuidTest1() {
			String guid=midToGuid("/m/0478__m");
			assertEquals("#9202a8c04000641f8000000008747ff3",guid);
		}
		
		@Test
		public void midToGuidTest2() {
			String guid=midToGuid("/m/05gml6t");
			assertEquals("#9202a8c04000641f800000000ae9c8d9",guid);
		}
		
		@Test
		public void guidToMidTest1() {
			String mid=guidToMid("#9202a8c04000641f8000000008747ff3");
			assertEquals("/m/0478__m",mid);
		};
		
		@Test
		public void guidToMidTest2() {
			String mid=guidToMid("#9202a8c04000641f80000000202ba22f");
			assertEquals("/m/0j2r8kh",mid);
		};

}
