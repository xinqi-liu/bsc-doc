package monitorUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.lang.Float;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject ;
//引入slf4j接口的Logger和LoggerFactory
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class ApiAlertUtilsLogParse {
    String line;
    final static Logger logger = LoggerFactory.getLogger(ApiAlertUtilsLogParse.class);
    public static Map<String,String> UtilsLogParse(String line) {
        Map<String,String> log = new HashMap<String,String>();
        try {
            String decoded = new String(line);
            JSONObject json = new JSONObject(decoded);
            log.put("apiId",json.getString("api_id"));
            log.put("requestTime",json.getString("pvCount_time"));
            log.put("httpCode",json.getString("status"));
        } catch (Exception e) {
            String errorMessage=String.format("ApiAlertUtilsLogParse:Error while processing %s",line);
            logger.error(errorMessage,e);
            log = null;
        } 
        return log;
    }
}
