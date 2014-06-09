package org.buildoop.storm.tools;

import java.util.Map;

public class AuditParser {
    public static Map<String,Object> parseAuditInput(String auditLine) {
    	
        
    	AuditHashMap attributes = new AuditHashMap();
    	
    	String[] auxAttributes = null;
    	
    	if (auditLine.contains("msg='")){
    		// Split string after ":" in three strings: before msg info, msg info and after msg info.
        	String beforeMsgInfo = auditLine.substring(0,auditLine.indexOf("msg='")-1); 
            String msgInfo = auditLine.substring(auditLine.indexOf("msg='"), auditLine.lastIndexOf("'")+1);
        	String afterMsgInfo = auditLine.substring( auditLine.lastIndexOf("'")+1);
        	
        	// Parse beforeMsgInfo
        	auxAttributes = beforeMsgInfo.split(" ");
        	attributes.insertNewKeyAndValuesOfStringArray(auxAttributes);
        	    	
        	// Parse msgInfo;
    		// Parse info between "'"
    		auxAttributes = msgInfo.substring(msgInfo.indexOf("'")+1,msgInfo.length()-1).split(" ");
    		attributes.insertNewKeyAndValuesOfStringArray(auxAttributes);
    		    	
        	// Parse afterMsgInfo
    		auxAttributes = afterMsgInfo.split(" ");
    		attributes.insertNewKeyAndValuesOfStringArray(auxAttributes);
    	}
    	else
    	{
    		auxAttributes = auditLine.split(" ");
    		attributes.insertNewKeyAndValuesOfStringArray(auxAttributes);
    	}
    	return attributes;
    }
}
