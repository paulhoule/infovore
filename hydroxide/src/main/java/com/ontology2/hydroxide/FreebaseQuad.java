package com.ontology2.hydroxide;

import static com.ontology2.basekb.StatelessIdFunctions.*;

public class FreebaseQuad {
    private final String subject;
    private final String property;
    private final String destination;
    private final String value;
    
    public FreebaseQuad(String subject,String property,String destination,String value) {
    	this.subject=subject;
    	this.property=property.intern();
    	this.destination=destination;
    	this.value=value;
    }
    
    public static FreebaseQuad createFromLine(String line) {
        String[] parts=line.split("\t");
        return new FreebaseQuad(
        		parts[0],
        		parts[1],
        		parts.length>2 ? parts[2] : "",
        		parts.length> 3 ? parts[3] : ""		
        );
    }
    
    public String getSubject() {
        return subject;
    }
    
    public long getSubjectAsLong() {
    	return midToLong(subject);
    }
    
    public String getProperty() {
        return property;
    }

    public String getDestination() {
        return destination;
    }

    public String getValue() {
        return value;
    }

    public static String unescapeFreebaseKey(String in) {
        StringBuffer out=new StringBuffer();
        String [] parts=in.split("[$]");
        out.append(parts[0]);
        for(int i=1;i<parts.length;i++) {
            String hexSymbols=parts[i].substring(0,4);
            String remainder="";
            if(parts[i].length()>4) {
                remainder=parts[i].substring(4);
            }
            
            int codePoint=Integer.parseInt(hexSymbols,16);
            char[] character=Character.toChars(codePoint);
            out.append(character);
            out.append(remainder);
        }
        
        return out.toString();
    }
    
    
    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append(subject);
        sb.append("\t");
        sb.append(property);
        sb.append("\t");
        sb.append(destination);
        sb.append("\t");
        sb.append(value);
        return sb.toString();
    }
}
