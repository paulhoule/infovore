package com.ontology2.bakemono.util;

import com.google.common.base.CharMatcher;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;



//
// Functions in this class implement transformations on identifiers that
// do not require a live knowledge base and that don't depend on
// a particular RDF library.
//

public class StatelessIdFunctions {

    //
    // Freebase guids and mids actually represent sequential integers;  these
    // functions transform them to and from integers so we can convert
    // between them and sort Freebase identifiers in the order they were
    // inserted
    //

    private static String b32digits="0123456789bcdfghjklmnpqrstvwxyz_";
    private static String bkNs="http://rdf.basekb.com/ns/";

    public static long midToLong(String mid) {
        long value=0;
        if(!mid.startsWith("/m/0")) {
            throw new IllegalArgumentException("ill-formed mid ["+mid+"]");
        }

        for(int i=4;i<mid.length();i++) {
            String c=mid.substring(i,i+1);
            int digitValue= b32digits.indexOf(c);
            if (digitValue==-1) {
                throw new IllegalArgumentException("ill-formed mid ["+mid+"]");
            }
            value = value << 5;
            value = value | digitValue;
        }

        return value;
    }

    public static String longToGuid(long l) {
        return "#9202a8c04000641f"+Long.toHexString(l | 0x8000000000000000l);
    }

    public static String midToGuid(String mid) {
        return longToGuid(midToLong(mid));
    }

    public static long guidToLong(String guid) {
        String guidPrefix="#9202a8c04000641f8";
        if (!guid.startsWith(guidPrefix)) {
            throw new IllegalArgumentException("Guid ["+guid+"] does not start with valid prefix");
        }

        String guidDigits=guid.substring(guidPrefix.length());
        if (15!=guidDigits.length()) {
            throw new IllegalArgumentException("Guid ["+guid+"] has wrong number of digits");
        }

        return Long.parseLong(guidDigits,16);
    };

    public static String longToMid(long numericId) {
        StringBuffer sb=new StringBuffer(16);
        while(numericId>0) {
            int digit=(int) (numericId % 32);
            sb.append(b32digits.charAt(digit));
            numericId=numericId/32;
        }

        sb.append("0/m/");
        return sb.reverse().toString();
    };

    public static String guidToMid(String guid) {
        return longToMid(guidToLong(guid));
    }

    public static String toBkUri(String fbId) {
        if ("".equals(fbId))
            return bkNs;

        return bkNs + fbId.substring(1).replace('/', '.');
    }

    // ----------------------------------------------------


    public static String iriEscape(String key){
        return new IRIEscaper().escape(key);
    }

    public static String dbpediaEscape(String key) {
        return new DBpediaEscaper().escape(key);
    }

    public static class IRIEscaper {
        StringBuffer out;

        public String escape(String key){
            out=new StringBuffer();
            final int length = key.length();
            for (int offset = 0; offset < length; ) {
                final int codepoint = key.codePointAt(offset);
                transformChar(codepoint);
                offset += Character.charCount(codepoint);
            }

            return out.toString();
        }

        private void transformChar(int cp) {
            char[] rawChars=Character.toChars(cp);
            if(acceptChar(rawChars,cp)) {
                out.append(Character.toChars(cp));
            } else {
                percentEncode(rawChars);
            }
        }

        private void percentEncode(char[] rawChars) {
            try {
                byte[] bytes=new String(rawChars).getBytes("UTF-8");
                for(byte b:bytes) {
                    out.append('%');
                    out.append(byteToHex(b));
                }
            } catch(UnsupportedEncodingException ex) {
                throw new RuntimeException(ex);
            }
        }

        static String byteToHex(byte b) {
            String padded="00"+Integer.toHexString(0x00FF & (int) b).toUpperCase();
            return padded.substring(padded.length()-2);
        }

        //
        // this code should implement the 'ipchar' production from
        //
        // http://www.apps.ietf.org/rfc/rfc3987.html
        //

        protected boolean acceptChar(char[] chars,int cp) {
            if(chars.length==1) {
                char c=chars[0];
                if(Character.isLetterOrDigit(c))
                    return true;

                if(c=='-' || c=='.' || c=='_' || c=='~')
                    return true;

                if(c=='!' || c=='$' || c=='&' || c=='\'' || c=='(' || c==')'
                        || c=='*' || c=='+' || c==',' || c==';' || c=='='
                        || c== ':' || c=='@')
                    return true;

                if (cp<0xA0)
                    return false;
            }

            if(cp>=0xA0 && cp<=0xD7FF)
                return true;

            if(cp>=0xF900 && cp<=0xFDCF)
                return true;

            if(cp>=0xFDF0 && cp<=0xFFEF)
                return true;

            if (cp>=0x10000 && cp<=0x1FFFD)
                return true;

            if (cp>=0x20000 && cp<=0x2FFFD)
                return true;

            if (cp>=0x30000 && cp<=0x3FFFD)
                return true;

            if (cp>=0x40000 && cp<=0x4FFFD)
                return true;

            if (cp>=0x50000 && cp<=0x5FFFD)
                return true;

            if (cp>=0x60000 && cp<=0x6FFFD)
                return true;

            if (cp>=0x70000 && cp<=0x7FFFD)
                return true;

            if (cp>=0x80000 && cp<=0x8FFFD)
                return true;

            if (cp>=0x90000 && cp<=0x9FFFD)
                return true;

            if (cp>=0xA0000 && cp<=0xAFFFD)
                return true;

            if (cp>=0xB0000 && cp<=0xBFFFD)
                return true;

            if (cp>=0xC0000 && cp<=0xCFFFD)
                return true;

            if (cp>=0xD0000 && cp<=0xDFFFD)
                return true;

            if (cp>=0xE1000 && cp<=0xEFFFD)
                return true;

            return false;
        }
    }

    public static class DBpediaEscaper extends IRIEscaper {
        // I looked at all the characters that actually appear in DBpedia 3.9 keys and removed the
        // % escape character.  Note that this seems to be the same as what acceptChar() accepts above
        // in the range 0..127,  which is of course what it should be

        final String observedCharacters="!$&'()*+,-.0123456789:;=@ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~";
        final CharMatcher cm=CharMatcher.anyOf(observedCharacters);

        protected boolean acceptChar(char[] chars,int cp) {
            if(chars.length==1) {
                return cm.matches(chars[0]);
            }

            return false;
        }
    }

    public static String unescapeKey(String key) {
        return new Unescaper().unescape(key);
    }

    private static class Unescaper {
        StringBuffer out;
        StringBuffer hexbytes;
        int state=0;

        public String unescape(String key) {
            out=new StringBuffer(key.length());
            for(int i=0;i<key.length();i++) {
                processChar(key.charAt(i));
            }
            return out.toString();
        }

        private void processChar(char charAt) {
            if (state==0) {
                if('$'==charAt) {
                    state=1;
                    hexbytes=new StringBuffer();
                } else {
                    out.append(charAt);
                }
            } else {
                hexbytes.append(charAt);
                if (state==4) {
                    int codepoint=Integer.parseInt(hexbytes.toString(),16);
                    char[] specialChar=Character.toChars(codepoint);
                    out.append(specialChar);
                    state=0;
                } else {
                    state++;
                }
            }
        }
    }

    public static int hashRawMid(String mid,int modulus) {
        byte[] hashResult=DigestUtils.md5(mid);
        long hashInt = hashArrayToInt(hashResult);
        return (int) Math.abs(hashInt % modulus);
    }

    public static long hashArrayToInt(byte[] hashResult) {
        return ByteBuffer.wrap(hashResult).getLong();
    }
}
