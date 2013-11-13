package com.ontology2.setOperations;

import com.ontology2.bakemono.setOperations.TaggedTextKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.*;

public class TaggedTextKeyTest {

    @Test
    public void zeroArgConstructorLeavesAllNull() {
        TaggedTextKey that=new TaggedTextKey();
        assertNull(that.getKey());
        assertNull(that.getTag());
    }

    @Test
    public void dualArgConstructorSetsValues() {
        TaggedTextKey that=new TaggedTextKey(
            new Text("Doctor Funkenstein")
            ,new VIntWritable(33550336)
        );

        assertEquals("Doctor Funkenstein",that.getKey().toString());
        assertEquals(33550336,that.getTag().get());
    }

    @Test
    public void equalIfTagsAreEqual() {
        TaggedTextKey k1=new TaggedTextKey(
                new Text("Doctor Funkenstein")
                ,new VIntWritable(33550336)
        );

        TaggedTextKey k2=new TaggedTextKey(
                new Text("Doctor Funkenstein")
                ,new VIntWritable(702)
        );

        assertTrue(k1.equals(k2));
    }

    @Test
    public void unequalIfTagsAreUnequal() {
        TaggedTextKey k1=new TaggedTextKey(
                new Text("Doctor Funkenstein")
                ,new VIntWritable(33550336)
        );

        TaggedTextKey k2=new TaggedTextKey(
                new Text("Sir Nose D'void of Funk")
                ,new VIntWritable(702)
        );

        assertFalse(k1.equals(k2));
    }

    @Test
    public void hashEqualsHashOfKey() {
        TaggedTextKey k1=new TaggedTextKey(
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
        TaggedTextKey k1=new TaggedTextKey(
                new Text("a")
                ,new VIntWritable(2)
        );

        TaggedTextKey k2=new TaggedTextKey(
                new Text("b")
                ,new VIntWritable(1)
        );

        assertEquals(-1,k1.compareTo(k2));
        assertEquals(1,k2.compareTo(k1));
    }

    @Test
    public void exactlyEqualSortsSame() {
        TaggedTextKey k1=new TaggedTextKey(
                new Text("a")
                ,new VIntWritable(2)
        );

        TaggedTextKey k2=new TaggedTextKey(
                new Text("a")
                ,new VIntWritable(2)
        );

        assertEquals(0,k1.compareTo(k2));
        assertEquals(0,k2.compareTo(k1));
    }

    @Test
    public void sortsOnTagIfKeysSame() {
        TaggedTextKey k1=new TaggedTextKey(
                new Text("a")
                ,new VIntWritable(1)
        );

        TaggedTextKey k2=new TaggedTextKey(
                new Text("a")
                ,new VIntWritable(2)
        );

        assertEquals(-1,k1.compareTo(k2));
        assertEquals(1,k2.compareTo(k1));
    }

    @Test
    public void roundTrips() throws IOException {
        TaggedTextKey k1=new TaggedTextKey(
                new Text("solvent")
                ,new VIntWritable(7777)
        );

        ByteArrayOutputStream byteStream=new ByteArrayOutputStream();
        DataOutput out=new DataOutputStream(byteStream);
        k1.write(out);
        ByteArrayInputStream inputBytes=new ByteArrayInputStream(byteStream.toByteArray());
        TaggedTextKey k2=new TaggedTextKey();
        k2.readFields(new DataInputStream(inputBytes));
        assertEquals(k1,k2);
        assertEquals("solvent",k1.getKey().toString());
        assertEquals(7777,k2.getTag().get());
    }
}
