package com.ontology2.bakemono.sumRDF;

import org.junit.Test;
import static com.ontology2.bakemono.sumRDF.SumRDFMapper.*;
import static junit.framework.TestCase.assertEquals;

public class StaticMapperTest {
    @Test
    public void q1() {
        assertEquals("foo", getQuoteContents("\"foo\""));
    }

    @Test
    public void q2() {
        assertEquals("", getQuoteContents("\"\""));
    }

    @Test
    public void q3() {
        assertEquals(null, getQuoteContents("foo"));
    }

    @Test
    public void q4() {
        assertEquals(null, getQuoteContents("\"foo"));
    }
}
