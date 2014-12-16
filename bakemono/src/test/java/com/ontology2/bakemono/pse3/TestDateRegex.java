package com.ontology2.bakemono.pse3;


import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class TestDateRegex {

    public boolean test(String s) {
        return PSE3Util.dateTimePattern().matcher(s).matches();
    }
    @Test
    public void wholeseconds() {
        assertTrue(test("2008-04-10T15:42:19"));
    }

    @Test
    public void fractionalSecond() {
        assertTrue(test("2008-04-10T15:42:19.88774"));
    }

    @Test
    public void baretimeDisallowed() {
        assertFalse(test("T15:42:19"));
    }

    @Test
    public void cantBeMisingHour() {
        assertFalse(test("2008-04-10T15:42"));
    }

}
