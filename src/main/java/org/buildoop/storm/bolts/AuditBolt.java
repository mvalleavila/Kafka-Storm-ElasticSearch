package org.buildoop.storm.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

import org.buildoop.storm.tools.AuditParser;

import static backtype.storm.utils.Utils.tuple;

@SuppressWarnings("serial")
public class AuditBolt implements IBasicBolt {

	@SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
    	System.out.println("!!!!!!ooooooooo!!!!!!!! ----- > AuditBolt: execute");
    	Map<String,String> tupleValue = fillAuditAttributes(AuditParser.parseAuditInput(input.getString(0)));
    	if (!tupleValue.isEmpty())
    		collector.emit(tuple(tupleValue));
    }

    private Map<String,String> fillAuditAttributes(Map<String,Object> attributes){
    	
    	Map<String,String> tupleValue = new HashMap<String,String>();
    	String type = null;
    	
    	if (attributes.containsKey("type")){
    		type = (String)attributes.get("type");
    		
    		switch (type){
    			case "USER_LOGIN":
    			case "USER_LOGOUT":
    				if (attributes.get("res").equals("success"))
    				{
    					tupleValue.put("type", type);
    		   			tupleValue.put("host", (String)attributes.get("node"));
    					tupleValue.put("user", (String)attributes.get("username"));
    				}
    			default:
    		}    		
    	}    	
    	return tupleValue;
    }

	public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tupleValue"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    

}