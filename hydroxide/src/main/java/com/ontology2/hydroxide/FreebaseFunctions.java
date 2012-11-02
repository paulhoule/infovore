package com.ontology2.hydroxide;


import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;

import org.apache.commons.codec.digest.DigestUtils;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.ontology2.basekb.BaseIRI;

//public class FreebaseFunctions {
//
//
//	public static RDFNode fromFb(Model m,String fbId) {
//		return m.asRDFNode(fromFb(fbId));
//	}
//	
//	public static Node fromFb(String fbId) {
//		if ("".equals(fbId))
//			return Node.createURI(BaseIRI.freebaseBase);
//		
//		return Node.createURI(BaseIRI.freebaseBase + fbId.substring(1).replace('/', '.'));
//	}
//	
//	public static RDFNode toBk(Model m,String fbId) {
//		return m.asRDFNode(toBk(fbId));
//	}
//	
//	public static Node toBk(String fbId) {
//		if ("".equals(fbId))
//			return Node.createURI(BaseIRI.bkNs);
//		
//		return Node.createURI(BaseIRI.bkNs + fbId.substring(1).replace('/', '.'));			
//	}
//	
//	public static Node toBkKeyProperty(String fbId) {
//		if ("".equals(fbId))
//			return Node.createURI(BaseIRI.bkNs);
//		
//		return Node.createURI(BaseIRI.bkPublic + "hasKey." + fbId.substring(1).replace('/', '.'));			
//	}
//	
//	public static RDFNode toBkKeyProperty(Model m,String fbId) {
//		return m.asRDFNode(toBkKeyProperty(fbId));
//	}
//	
//	public static RDFNode toBkProperty(Model m,String fbId) {
//		return m.createProperty(BaseIRI.bkNs + fbId.substring(1).replace('/', '.'));
//	}
//	
//	
//	public static boolean isMid(RDFNode r) {
//		if(!r.isResource())
//			return false;
//		
//		String uri=((Resource) r).getURI();
//		return uri.startsWith(BaseIRI.freebaseBase+"m.");
//	}
//	
//	public static String toFb(Node fb) {
//		String url=fb.toString();
//		if(url.startsWith(BaseIRI.bkNs)) {
//			return "/"+url.substring(BaseIRI.bkNs.length()).replace(".", "/");			
//		}
//		
//		if(url.startsWith(BaseIRI.freebaseBase)) {
//			return "/"+url.substring(BaseIRI.freebaseBase.length()).replace(".", "/");			
//		}
//		
//		return null;
//	}
//	
//	public static String toFb(RDFNode fb) {
//		return toFb(fb.asNode());
//	}
//
//	public static String iriEscape(String key){
//		return new IRIEscaper().escape(key);
//	}
//	
//	public static class IRIEscaper {
//		StringBuffer out;
//
//		public String escape(String key){
//			out=new StringBuffer();
//			final int length = key.length();
//			for (int offset = 0; offset < length; ) {
//			   final int codepoint = key.codePointAt(offset);
//			   transformChar(codepoint);
//			   offset += Character.charCount(codepoint);
//			}
//
//			return out.toString();
//		}
//		
//		private void transformChar(int cp) {
//			char[] rawChars=Character.toChars(cp);
//			if(acceptChar(rawChars,cp)) {
//				out.append(Character.toChars(cp));
//			} else {
//				percentEncode(rawChars);
//			}
//		}
//		
//		private void percentEncode(char[] rawChars) {
//			try {
//				byte[] bytes=new String(rawChars).getBytes("UTF-8");
//				for(byte b:bytes) {
//					out.append('%');
//					out.append(byteToHex(b));
//				}
//			} catch(UnsupportedEncodingException ex) {
//				throw new RuntimeException(ex);
//			}
//		}
//
//		static String byteToHex(byte b) {
//			String padded="00"+Integer.toHexString(0x00FF & (int) b).toUpperCase();
//			return padded.substring(padded.length()-2);
//		}
//		
//		//
//		// this code should implement the 'ipchar' production from
//		//
//		// http://www.apps.ietf.org/rfc/rfc3987.html
//		//
//		
//		private boolean acceptChar(char[] chars,int cp) {
//			if(chars.length==1) {
//				char c=chars[0];
//				if(Character.isLetterOrDigit(c))
//					return true;
//			
//				if(c=='-' || c=='.' || c=='_' || c=='~')
//					return true;
//			
//				if(c=='!' || c=='$' || c=='&' || c=='\'' || c=='(' || c==')' 
//					|| c=='*' || c=='+' || c==',' || c==';' || c=='='
//						|| c== ':' || c=='@')
//					return true;
//			
//				if (cp<0xA0)
//					return false;
//			}
//			
//			if(cp>=0xA0 && cp<=0xD7FF)
//				return true;
//			
//			if(cp>=0xF900 && cp<=0xFDCF)
//				return true;
//			
//			if(cp>=0xFDF0 && cp<=0xFFEF)
//				return true;
//			
//			if (cp>=0x10000 && cp<=0x1FFFD)
//				return true;
//			
//			if (cp>=0x20000 && cp<=0x2FFFD)
//				return true;
//			
//			if (cp>=0x30000 && cp<=0x3FFFD)
//				return true;
//			
//			if (cp>=0x40000 && cp<=0x4FFFD)
//				return true;
//			
//			if (cp>=0x50000 && cp<=0x5FFFD)
//				return true;
//			
//			if (cp>=0x60000 && cp<=0x6FFFD)
//				return true;
//			
//			if (cp>=0x70000 && cp<=0x7FFFD)
//				return true;
//			
//			if (cp>=0x80000 && cp<=0x8FFFD)
//				return true;
//			
//			if (cp>=0x90000 && cp<=0x9FFFD)
//				return true;
//			
//			if (cp>=0xA0000 && cp<=0xAFFFD)
//				return true;
//			
//			if (cp>=0xB0000 && cp<=0xBFFFD)
//				return true;
//			
//			if (cp>=0xC0000 && cp<=0xCFFFD)
//				return true;
//			
//			if (cp>=0xD0000 && cp<=0xDFFFD)
//				return true;
//			
//			if (cp>=0xE1000 && cp<=0xEFFFD)
//				return true;
//			
//			return false;
//		}
//	}
//	
//	
//	public static String unescapeKey(String key) {
//		return new Unescaper().unescape(key);
//	}
//	
//	private static class Unescaper {
//		StringBuffer out;
//		StringBuffer hexbytes;
//		int state=0;
//		
//		public String unescape(String key) {
//			out=new StringBuffer(key.length());
//			for(int i=0;i<key.length();i++) {
//				processChar(key.charAt(i));
//			}
//			return out.toString();
//		}
//
//		private void processChar(char charAt) {
//			if (state==0) {
//				if('$'==charAt) {
//					state=1;
//					hexbytes=new StringBuffer();
//				} else {
//					out.append(charAt);
//				}
//			} else {
//				hexbytes.append(charAt);
//				if (state==4) {
//					int codepoint=Integer.parseInt(hexbytes.toString(),16);
//					char[] specialChar=Character.toChars(codepoint);
//					out.append(specialChar);
//					state=0;
//				} else {
//					state++;
//				}
//			}
//		}
//	}
//	
//	public static int hashRawMid(String mid,int modulus) {
//		byte[] hashResult=DigestUtils.md5(mid);
//		long hashInt = hashArrayToInt(hashResult);
//		return (int) Math.abs(hashInt % modulus);
//	}
//	
//	public static long hashArrayToInt(byte[] hashResult) {
//		return ByteBuffer.wrap(hashResult).getLong();
//	}
//}
