package com.ontology2.bakemono.pse3;
import org.junit.Test;

import static com.ontology2.bakemono.pse3.PSE3Mapper.unescapeFreebaseKey;
import static junit.framework.TestCase.assertEquals;

public class TestUnescape {
    @Test
    public void passThru() {
        assertEquals("cover me",unescapeFreebaseKey("cover me"));
    }

    @Test
    public void passThruEmpty() {
        assertEquals("",unescapeFreebaseKey(""));
    }

    @Test
    public void alternateA() {
        assertEquals("A",unescapeFreebaseKey("$0041"));
    }

    @Test
    public void alternateAB() {
        assertEquals("AB",unescapeFreebaseKey("$0041$0042"));
    }

    @Test
    public void alternateABC() {
        assertEquals("ABC",unescapeFreebaseKey("$0041B$0043"));
    }
}
