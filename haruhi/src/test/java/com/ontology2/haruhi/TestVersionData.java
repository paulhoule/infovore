package com.ontology2.haruhi;

import com.google.common.base.CharMatcher;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.*;

public class TestVersionData {

    @Test
    public void iHaveAVersionNumber() throws IOException {
        InputStream in=getClass().getResourceAsStream("version.properties");
        Properties p= new Properties();
        p.load(in);
        String value=p.getProperty("com.ontology2.haruhi.version");
        assertNotNull(value);
        assertNotEquals("",value);
        assertTrue(CharMatcher.DIGIT.matchesAnyOf(value));
    }
}
