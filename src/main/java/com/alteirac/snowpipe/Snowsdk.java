package com.alteirac.snowpipe;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.JSONObject;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;


public class Snowsdk {
  private static String PROFILE_PATH = "param_snow.json";
  private static final ObjectMapper mapper = new ObjectMapper();
  private SnowflakeStreamingIngestChannel channel = null;

  public SnowflakeStreamingIngestChannel getChannel(){
    return this.channel;
  }

  public void connect() throws Exception{
    Properties props = new Properties();
    Iterator<Map.Entry<String, JsonNode>> propIt =
        mapper.readTree(new String(Files.readAllBytes(Paths.get(PROFILE_PATH)))).fields();
    while (propIt.hasNext()) {
      Map.Entry<String, JsonNode> prop = propIt.next();
      props.put(prop.getKey(), prop.getValue().asText());
    }
    SnowflakeStreamingIngestClient client =
        SnowflakeStreamingIngestClientFactory.builder("MY_CLIENT").setProperties(props).build(); 
        OpenChannelRequest request1 =
              OpenChannelRequest.builder("MY_CHANNEL")
                  .setDBName("SNOWPIPE_STREAMING")
                  .setSchemaName("DEV")
                  .setTableName("FROMSDK")
                  .setOnErrorOption(
                      OpenChannelRequest.OnErrorOption.CONTINUE) // Another ON_ERROR option is ABORT
                  .build();

          // Open a streaming ingest channel from the given client
      this.channel= client.openChannel(request1);
  }

  public void sendMessage(Map<String, Object> msg){
    if(this.channel==null){
      try {
        this.connect();
      } catch (Exception e) {
        System.out.println("ERROR " + e.toString());
      }
    
    }
    String offsetTokenFromSnowflake = this.channel.getLatestCommittedOffsetToken();
    InsertValidationResponse response = this.channel.insertRow(msg, String.valueOf(offsetTokenFromSnowflake+1));
    if (response.hasErrors()) {
      throw response.getInsertErrors().get(0).getException();
    }
  }
  public static void main(String[] args) throws Exception {
    Snowsdk s=new Snowsdk();
    Map<String, Object> row = new HashMap<String, Object>();
    JSONObject obj=new JSONObject();    
    obj.put("name","sonoo");    
    obj.put("age",27);    
    obj.put("salary",6000); 
    row.put("C1", obj);
    s.sendMessage(row);
    s.getChannel().close().get();

  }
}
