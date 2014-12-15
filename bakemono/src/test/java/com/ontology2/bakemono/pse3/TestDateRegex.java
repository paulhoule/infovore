package com.ontology2.bakemono.pse3;


import org.junit.Test;

import java.util.function.Predicate;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class TestDateRegex {
    Predicate<String> that=PSE3Util.dateTimePattern().asPredicate();
    @Test
    public void wholeseconds() {
        assertTrue(that.test("2008-04-10T15:42:19"));
    }

    @Test
    public void fractionalSecond() {
        assertTrue(that.test("2008-04-10T15:42:19.88774"));
    }

    @Test
    public void baretimeDisallowed() {
        assertFalse(that.test("T15:42:19"));
    }

    @Test
    public void cantBeMisingHour() {
        assertFalse(that.test("2008-04-10T15:42"));
    }

}
