package org.buildoop.storm.tools;

import java.util.HashMap;

@SuppressWarnings("serial")
public class AuditHashMap extends HashMap<String, Object> {
	
	public AuditHashMap(){
		super();
	}
	
	public AuditHashMap insertNewKeyAndValuesOfStringArray(String[] stringArray){
		
		String key = null;
		Object value = null;
		
    	for (int i=0; i < stringArray.length; i++){
    		if (stringArray[i].contains("=")){
    			key = stringArray[i].split("=")[0];
    			value = stringArray[i].split("=")[1];
    			this.put(key, value);
    		}
    	}		
		return this;
	}
	
}
