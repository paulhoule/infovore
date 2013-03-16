package com.ontology2.hydroxide.shell;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class TestInfovoreShell {

	@Before
	public void setup() {
		ShellTestApp.reset();
	}
	
	@Test
	public void test() throws IOException {
		String[] arguments = {"run","ShellTest"};
		assertFalse(ShellTestApp.getGotHit());
		InfovoreShell.main(arguments);
		assertTrue(ShellTestApp.getGotHit());	
		assertEquals(0,ShellTestApp.getLastArguments().length);
	}
	
	@Test
	public void testArgs() throws IOException {
		String[] arguments = {"run","ShellTest","panic","in","detroit"};
		assertFalse(ShellTestApp.getGotHit());
		InfovoreShell.main(arguments);
		assertTrue(ShellTestApp.getGotHit());	
		assertEquals(3,ShellTestApp.getLastArguments().length);
		assertEquals("panic",ShellTestApp.getLastArguments()[0]);
		assertEquals("in",ShellTestApp.getLastArguments()[1]);
		assertEquals("detroit",ShellTestApp.getLastArguments()[2]);
	}
	
	@Test
	public void testSingleArg() throws IOException {
		String[] arguments = {"run","ShellTest","one"};
		assertFalse(ShellTestApp.getGotHit());
		InfovoreShell.main(arguments);
		assertTrue(ShellTestApp.getGotHit());	
		assertEquals(1,ShellTestApp.getLastArguments().length);
		assertEquals("one",ShellTestApp.getLastArguments()[0]);
	}

}
