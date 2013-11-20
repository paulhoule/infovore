package com.ontology2.bakemono.joins;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.*;

public class TaggedTextKeyTest {

    @Test
    public void zeroArgConstructorLeavesAllNull() {
        TaggedTextItem that=new TaggedTextItem();
        assertNull(that.getKey());
        assertNull(that.getTag());
    }

    @Test
    public void dualArgConstructorSetsValues() {
        TaggedTextItem that=new TaggedTextItem(
            new Text("Doctor Funkenstein")
            ,new VIntWritable(33550336)
        );

        assertEquals("Doctor Funkenstein",that.getKey().toString());
        assertEquals(33550336,that.getTag().get());
    }

    @Test
    public void equalIfTagsAreEqual() {
        TaggedTextItem k1=new TaggedTextItem(
                new Text("Doctor Funkenstein")
                ,new VIntWritable(33550336)
        );

        TaggedTextItem k2=new TaggedTextItem(
                new Text("Doctor Funkenstein")
                ,new VIntWritable(702)
        );

        assertTrue(k1.equals(k2));
    }

    @Test
    public void unequalIfTagsAreUnequal() {
        TaggedTextItem k1=new TaggedTextItem(
                new Text("Doctor Funkenstein")
                ,new VIntWritable(33550336)
        );

        TaggedTextItem k2=new TaggedTextItem(
                new Text("Sir Nose D'void of Funk")
                ,new VIntWritable(702)
        );

        assertFalse(k1.equals(k2));
    }

    @Test
    public void hashEqualsHashOfKey() {
        TaggedTextItem k1=new TaggedTextItem(
                new Text("Doctor Funkenstein")
                ,new VIntWritable(33550336)
        );

        assertEquals(
                new Text("Doctor Funkenstein").hashCode()
                ,k1.hashCode()
        );
    }

    @Test
    public void sortsByOrderOfKeys() {
        TaggedTextItem k1=new TaggedTextItem(
                new Text("a")
                ,new VIntWritable(2)
        );

        TaggedTextItem k2=new TaggedTextItem(
                new Text("b")
                ,new VIntWritable(1)
        );

        assertEquals(-1,k1.compareTo(k2));
        assertEquals(1,k2.compareTo(k1));
    }

    @Test
    public void exactlyEqualSortsSame() {
        TaggedTextItem k1=new TaggedTextItem(
                new Text("a")
                ,new VIntWritable(2)
        );

        TaggedTextItem k2=new TaggedTextItem(
                new Text("a")
                ,new VIntWritable(2)
        );

        assertEquals(0,k1.compareTo(k2));
        assertEquals(0,k2.compareTo(k1));
    }

    @Test
    public void sortsOnTagIfKeysSame() {
        TaggedTextItem k1=new TaggedTextItem(
                new Text("a")
                ,new VIntWritable(1)
        );

        TaggedTextItem k2=new TaggedTextItem(
                new Text("a")
                ,new VIntWritable(2)
        );

        assertEquals(-1,k1.compareTo(k2));
        assertEquals(1,k2.compareTo(k1));
    }

    @Test
    public void roundTrips() throws IOException {
        TaggedTextItem k1=new TaggedTextItem(
                new Text("solvent")
                ,new VIntWritable(7777)
        );

        ByteArrayOutputStream byteStream=new ByteArrayOutputStream();
        DataOutput out=new DataOutputStream(byteStream);
        k1.write(out);
        ByteArrayInputStream inputBytes=new ByteArrayInputStream(byteStream.toByteArray());
        TaggedTextItem k2=new TaggedTextItem();
        k2.readFields(new DataInputStream(inputBytes));
        assertEquals(k1,k2);
        assertEquals("solvent", k1.getKey().toString());
        assertEquals(7777, k2.getTag().get());
    }
}
