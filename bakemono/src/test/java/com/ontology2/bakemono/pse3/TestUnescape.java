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

    @Test
    public void duckling() {
        assertEquals("$duckling",unescapeFreebaseKey("$duckling"));
    }

    @Test
    public void justaDollar() {
        assertEquals("$",unescapeFreebaseKey("$"));
    }

    @Test
    public void threeHexDigits() {
        assertEquals("$ABC",unescapeFreebaseKey("$ABC"));
    }

    @Test
    public void handlesPeripheralNonHex() {
        assertEquals("ABC",unescapeFreebaseKey("A$0042C"));
    };

    //
    // unescapeFreebaseKey is NOT required to process sequences that are not real unicode
    // characters,  it is not fair pool to text hex digit parsing if you don't use a real
    // character
    //

    //
    //  の
    //  JAPANESE HIRAGANA LETTER NO
    //  Codepoint is 12398
    //

    @Test
    public void hiraganaLetterNo() {
        assertEquals(0x306E,unescapeFreebaseKey("$306E").charAt(0));
    };

    //
    // SHIFT OUT ISO CONTROL CHARACTER
    //

    @Test
    public void tryLetterE() {
        assertEquals(0x000E,unescapeFreebaseKey("$000E").charAt(0));
    };

    //
    // ATAK LETTER MANDAILING NA
    //

    @Test
    public void tryHexLettersBCAAndNonzeroPosition3() {
        assertEquals(0x1BCA,unescapeFreebaseKey("$1BCA").charAt(0));
    };

    //
    // 퇹
    // HANGUL SYLLABLE-D1F9
    //

    @Test
    public void tryHexDFand9AndNegative() {
        assertEquals(0xD1F9,unescapeFreebaseKey("$D1F9").charAt(0));
    }
}
