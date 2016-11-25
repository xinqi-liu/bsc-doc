package monitorUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.JsonNode ;
import com.mashape.unirest.http.HttpResponse;
import org.json.JSONObject;
import org.json.JSONException;
import org.json.JSONArray;
import org.json.JSONStringer;
import org.json.JSONTokener;
//引入slf4j接口的Logger和LoggerFactory
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class ApiAlertUtilsConfigParse {
    final static Logger logger = LoggerFactory.getLogger(ApiAlertUtilsConfigParse.class);
    
    public String getUrl;
    
    public ArrayList<ApiAlertUtilsConfigModel> configs;
    
    public static JSONArray UtilsGetContent(String getUrl) {
        JSONArray configsJson;
        try {
            configsJson = Unirest.get(getUrl).asJson().getBody().getObject().getJSONArray("response");;
        }catch(Exception e){
            configsJson = null;
            String errorMessage=String.format("UtilsGetContent:Error while get %s",getUrl);
        }
        return configsJson;
    }
    public static ArrayList<ApiAlertUtilsConfigModel> UtilsConfigParse(JSONArray configsJson) {
        ArrayList<ApiAlertUtilsConfigModel> configs = new ArrayList<ApiAlertUtilsConfigModel>();
        if(configsJson==null) {	
            configs = null;
        }
        else {
            //JSONArray configsJson = new JSONArray(configsStr);
            
            for(int i=0; i<configsJson.length(); i++) {
            	JSONObject configJson = new JSONObject(configsJson.getString(i));
            	String period = configJson.getString("period");
            	String indicatorId = configJson.getString("indicatorId");
            	String strategyId = configJson.getString("strategyId");
            	JSONArray apiIdList = configJson.getJSONArray("apiId");
                String codeSet;
            	//遍历条件获取contentList
                JSONArray content = configJson.getJSONArray("content");
                ArrayList<Map<String,String>> contentList =  new ArrayList<Map<String,String>>();
            	for (int j=0; j<content.length(); j++ ) {
                    Map<String,String> contentMap = new HashMap<String,String>();
                    
                	JSONObject cond = new JSONObject(content.getString(j));
                	String indicatorSet = cond.getString("indicatorSet");
                	String compare = cond.getString("compare");
                	String number = cond.getString("number");
                	if(cond.getString("codeSet")!=null) {
                        codeSet = cond.getString("codeSet");
                	}
                	else{
                    	codeSet = null;
                	}
                    contentMap.put("indicatorSet",indicatorSet);
                    contentMap.put("compare",compare);
                    contentMap.put("number",number);
                    contentMap.put("codeSet",codeSet);
                    contentList.add(contentMap);
                }
                //遍历config中的apiId
                for (int k=0; k<apiIdList.length(); k++) {
                    String apiId = apiIdList.getString(k);
                    ApiAlertUtilsConfigModel config = new ApiAlertUtilsConfigModel();
                    config.period = period;
                    config.indicatorId = indicatorId;
                    config.strategyId = strategyId;
                    config.apiId = apiId;
                    config.content = contentList;
                    configs.add(config);	
                }
            }
        }
        System.out.println(configs);
        return configs;	
    }
}
