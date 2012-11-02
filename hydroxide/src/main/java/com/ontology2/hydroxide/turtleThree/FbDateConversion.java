package com.ontology2.hydroxide.turtleThree;

import java.util.regex.Pattern;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;

public class FbDateConversion {
	private final static Pattern yearRegex=Pattern.compile("^-?[0-9]{4}$");
	private final static Pattern yearMonthRegex=Pattern.compile("^-?[0-9]{4}-[0-9]{2}$");
	private final static Pattern dateRegex=Pattern.compile("^-?[0-9]{4}-[0-9]{2}-[0-9]{2}$");
	
	private final static Pattern hourDatedRegex=Pattern.compile("^-?[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}$");
	private final static Pattern hourMinuteDatedRegex=Pattern.compile("^-?[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}$");
	
	private final static Pattern hourTime=Pattern.compile("^T[0-9]{2}$");
	private final static Pattern hourMinuteTime=Pattern.compile("^T[0-9]{2}:[0-9]{2}$");
	
	static Node convertFreebaseDate(String value) {
		String isoDate=value.replaceFirst(" ", "T").replace("Z", "");
		
		if (yearRegex.matcher(value).matches()) {
			isoDate=value+"-01-01";
		}
		
		if (yearMonthRegex.matcher(value).matches()) {
			isoDate=value+"-01";
		}
		
		if (dateRegex.matcher(value).matches()) {
			isoDate=value;
		}
		
		if (hourDatedRegex.matcher(isoDate).matches()) {
			isoDate=isoDate+":00:00";
		}
		
		if (hourMinuteDatedRegex.matcher(isoDate).matches()) {
			isoDate=isoDate+":00";
		}
		
		if(hourTime.matcher(isoDate).matches()) {
			isoDate=isoDate+":00:00";
		}
		
		if(hourMinuteTime.matcher(isoDate).matches()) {
			isoDate=isoDate+":00";
		}
		
		if(!isoDate.contains("T")) {
			return Node.createLiteral(isoDate,XSDDatatype.XSDdate);
		}
		if(isoDate.startsWith("T")) {
			return Node.createLiteral(isoDate.substring(1),XSDDatatype.XSDtime);
		}
		
		return Node.createLiteral(isoDate,XSDDatatype.XSDdateTime);
	}
	
	static boolean isValidDate(Node n) {
		if (n.getLiteralDatatype()!=XSDDatatype.XSDdateTime
				&& n.getLiteralDatatype()!=XSDDatatype.XSDdate
				&& n.getLiteralDatatype()!=XSDDatatype.XSDtime) {
			return false;
		}
		
		try {
			Object o=n.getLiteralValue();
			return true;
		} catch(Exception ex) {
			return false;
		}
	}
}
