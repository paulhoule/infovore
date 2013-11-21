package com.ontology2.bakemono.joins;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestTaggedItemMatcher {
    private TaggedItemMatcher<Text> that;

    @Before
    public void setup() {
        that = new TaggedItemMatcher<Text>(new TaggedTextItem("an original population", 5));
    }

    @Test
    public void everythingMatches() {
        assertTrue(that.matches(new TaggedTextItem("an original population",5)));
    }

    @Test
    public void differentItem() {
        assertFalse(that.matches(new TaggedTextItem("the original population",5)));
    }

    @Test
    public void differentTag() {
        assertFalse(that.matches(new TaggedTextItem("an original population",6)));
    }
}
