package com.ontology2.basekb;

import static com.ontology2.basekb.StatelessIdFunctions.iriEscape;
import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.junit.Test;

import com.ontology2.basekb.StatelessIdFunctions.IRIEscaper;

public class TestIRIEscaper {
		 
		@Test
		public void test0001() {
			shouldPassthrough("ordinarystring");
		}
		
		@Test
		public void test0002() {
			shouldPassthrough("ordinary!string");
		}
		
		@Test
		public void test0003() {
			shouldPassthrough("ordinary_string");
		}
		
		@Test
		public void test0004() {
			assertEquals("ordinary%20string",iriEscape("ordinary string"));
		}
		
		@Test
		public void test0005() {
			shouldPassthrough("ordinary+string");
		}
		
		@Test
		public void test0006() {
			shouldPassthrough("お返し");
		}
		
		@Test
		public void test0007() {
			shouldPassthrough("復讐");
		}
		
		@Test
		public void test0008() {
			shouldPassthrough("復讐");
		}
		
		@Test
		public void test0009() {
			shouldPassthrough("(parenthesis_allowed)");
		}
		
		@Test
		public void test0010() {
			assertEquals("encode%2Fslash",iriEscape("encode/slash"));
		}
		
		@Test
		public void test0011()  {
			assertEquals("encode%2Fslash",iriEscape("encode/slash"));
		}
		
		@Test
		public void test0012()  {
			shouldPassthrough(":colon");
		}
		
		@Test
		public void test0013()  {
			sameAsBuiltin("<angle-brackets>");
		}
		
		@Test
		public void test0014()  {
			sameAsBuiltin("<angle-brackets>");
		}
		
		//
		// artificial example...
		//
		
		@Test
		public void test0015()  {
			sameAsBuiltin("\uFDEF");
		}

		@Test
		public void test0016() {
			byte b=5;
			String hex=IRIEscaper.byteToHex(b);
			assertEquals("05",hex);
		}
		
		@Test
		public void test0017() {
			byte b=(byte) 0xff;
			String hex=IRIEscaper.byteToHex(b);
			assertEquals("FF",hex);
		}
		
		@Test
		public void test0018() {
			String hex=IRIEscaper.byteToHex((byte) 0x4b);
			assertEquals("4B",hex);
		}
		
		private void shouldPassthrough(String sample) {
			assertEquals(sample,iriEscape(sample));
		}
		
		private void sameAsBuiltin(String sample) {
			try {
				assertEquals(URLEncoder.encode(sample,"UTF-8"),iriEscape(sample));
			} catch(UnsupportedEncodingException ex) {
				throw new RuntimeException(ex);
			}
		}
	}
