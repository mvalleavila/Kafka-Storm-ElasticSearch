/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.buildoop.storm.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.hmsonline.storm.elasticsearch.StormElasticSearchConstants;

import java.util.Map;
import java.util.Properties;

import static backtype.storm.utils.Utils.tuple;


@SuppressWarnings("serial")
public class AuditLoginsCounterBolt implements IBasicBolt {

	private Client client;
	private String index;
	private String type;	
	
	public AuditLoginsCounterBolt(){
		super();
	}


    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {    	
    	
    	String elasticSearchHost = (String) stormConf.get(StormElasticSearchConstants.ES_HOST);
        Integer elasticSearchPort = ((Long) stormConf.get(StormElasticSearchConstants.ES_PORT)).intValue();
        String elasticSearchCluster = (String) stormConf.get(StormElasticSearchConstants.ES_CLUSTER_NAME);
        
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", elasticSearchCluster).build();
        
        client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(
                elasticSearchHost, elasticSearchPort));
        
    	index = (String) stormConf.get("elasticsearch.index");
    	this.type = (String) stormConf.get("elasticsearch.type");
    }

    /*
     * Just output the word value with a count of 1.
     * The HBaseBolt will handle incrementing the counter.
     */
    public void execute(Tuple input, BasicOutputCollector collector){
    	
    	Map<String,String> auditAttributes = (Map<String, String>) input.getValues().get(0);
    	
    	String host_user = new String(auditAttributes.get("host")).concat("|")
    			.concat(auditAttributes.get("user"));
    	String type = auditAttributes.get("type");
    	int counterValue = getCounterValue(host_user);
    	String document = "{\"counter\": ";
    	    	
    	if (type.equals("USER_LOGIN")){
    		counterValue++;
    		document = document + counterValue + "}";
          	collector.emit(tuple(host_user,index, this.type, document));
    	} else
		if (type.equals("USER_LOGOUT") && counterValue > 0){
			counterValue--;
			document = document + counterValue + "}";
			collector.emit(tuple(host_user, index, this.type, document));
		}
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "index", "type", "document"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    
    private int getCounterValue(String host_user){

        try{
        	GetResponse response = client.prepareGet(index, type, host_user)
        	        .execute()
        	        .actionGet();
        	
        	Map<String, Object> test = response.getSource();
        	
        	return (int)test.get("counter");
        }
        catch (Exception e){
        	System.out.println("Error in get elasticSearch get: maybe index is still not created");
        	return 0;
        }
    }
}
