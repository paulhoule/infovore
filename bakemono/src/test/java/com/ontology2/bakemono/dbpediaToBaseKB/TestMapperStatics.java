package com.ontology2.bakemono.dbpediaToBaseKB;

import org.junit.Test;
import static com.ontology2.bakemono.dbpediaToBaseKB.DBpediaToBaseKBMapper.mapKey;
import static junit.framework.TestCase.assertEquals;

public class TestMapperStatics {
    @Test
    public void aSimpleCase() {
        assertEquals("Zillow",mapKey("Zillow"));
    }

    @Test
    public void transparentEscape() {
        assertEquals("A",mapKey("$0041"));
    }

    @Test
    public void transparentEscape2() {
        assertEquals("ABC",mapKey("$0041BC"));
    }

    @Test
    public void bangBangBang() {
        assertEquals("!!!",mapKey("$0021$0021$0021"));
    }

    @Test
    public void isoLatinOne() {
        assertEquals("Asociaci%C3%B3n_Alumni",mapKey("Asociaci$00F3n_Alumni"));
    }
}
