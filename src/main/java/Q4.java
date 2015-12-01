import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.RoutingContext;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yudun on 15/11/7.
 */
public class Q4 {
    final static String resultHeader = ConfigSingleton.TEAMID + "," +
            ConfigSingleton.TEAM_AWS_ACCOUNT_ID + "\n";

    static ConfigSingleton config = ConfigSingleton.getInstance();


    public static void lookupMysql(SQLConnection connection,
                                   String hashtag, String n,
                                   RoutingContext routingContext){

        StringBuilder result = new StringBuilder(resultHeader);

//        if (config.q4SuperCache.containsKey(hashtag)) {
//            ArrayList<String> array = config.q4SuperCache.get(hashtag);
//
//            int num = Integer.parseInt(n);
//            int arraylength = array.size();
//
//            for (int i = 0; i < num && i < arraylength; i++) {
//                result.append(array.get(i));
//            }
//
//            routingContext.response()
//                    .putHeader("Connection", "keep-alive")
//                    .putHeader("Content-Type", "text/plain;charset=UTF-8")
//                    .end(result.toString());
//        }
//        else {
            connection.query("SELECT "+config.mysqlQ4attrContent +
                    " FROM " + config.mysqlQ4MainTableName +
                    " WHERE " + config.mysqlQ4attrHashTag+ "=\'" + hashtag + "\'", res2 -> {


                if (res2.succeeded()) {

//                        System.out.println("\n\nhashtag=" + hashtag + ":" + "n=" + n +"\n\n");

                    List<JsonObject> rows = res2.result().getRows();

                    if (rows.size() == 0){
                        routingContext.response()
                                .putHeader("Connection", "keep-alive")
                                .putHeader("Content-Type", "text/plain;charset=UTF-8")
                                .end(result.toString());
                    }
                    else {
                        StringBuilder value = new StringBuilder();
                        for (JsonObject row : rows) {
                            value.append(row.getString(config.mysqlQ4attrContent) );
                        }

//                System.out.println("\n\n" + value.toString() + "\n\n");

                        try {
                            JSONArray array = new JSONArray(value.toString());
                            int num = Integer.parseInt(n);
                            int arraylength = array.length();

                            // update the cache
//                            if (arraylength > 0) {
//                                config.q4SuperCache.put(hashtag, array);
//                            }

                            for(int i = 0; i < num && i < arraylength ; i++){
                                JSONObject jsonObj  = array.getJSONObject(i);
                                result.append(jsonObj.getString("a") + "\n");
                            }

                        } catch (JSONException e) {
                            e.printStackTrace();
                        }


                        routingContext.response()
                                .putHeader("Connection", "keep-alive")
                                .putHeader("Content-Type", "text/plain;charset=UTF-8")
                                .end(result.toString());
                    }
                }
            });
//        }

    }


    public static void lookupHbase(String hashtag, String n,
                                   RoutingContext routingContext) {
        StringBuilder result = new StringBuilder(resultHeader);

//        if (config.q4SuperCache.containsKey(hashtag)) {
//            JSONArray array = config.q4SuperCache.get(hashtag);
//
//            int num = Integer.parseInt(n);
//            int arraylength = array.length();
//
//            try {
//                for (int i = 0; i < num && i < arraylength; i++) {
//                    JSONObject jsonObj = array.getJSONObject(i);
//                    result.append(jsonObj.getString("a") + "\n");
//                }
//
//                routingContext.response()
//                        .putHeader("Connection", "keep-alive")
//                        .putHeader("Content-Type", "text/plain;charset=UTF-8")
//                        .end(result.toString());
//
//            } catch (JSONException e) {
//                e.printStackTrace();
//                e.printStackTrace();
//                routingContext.response()
//                        .putHeader("Connection", "keep-alive")
//                        .putHeader("Content-Type", "text/plain;charset=UTF-8")
//                        .end(resultHeader);
//            }
//        }
//        else {
            Get query = new Get(Bytes.toBytes(hashtag));
            query.addFamily(Bytes.toBytes(config.hbaseQ4FamilyName));

            try {
                query.setMaxVersions(3);
                Result res = config.hbaseQ4Table.get(query);

                String target = Bytes.toString(res.getValue(Bytes.toBytes(config.hbaseQ4FamilyName),
                        Bytes.toBytes(config.hbaseQ4ColumnName))) + "\n";

                JSONArray array = new JSONArray(target);

                int num = Integer.parseInt(n);
                int arraylength = array.length();

                // update the cache
//                if (arraylength > 0) {
//                    config.q4SuperCache.put(hashtag, array);
//                }

                for(int i = 0; i < num && i < arraylength; i++){
                    JSONObject jsonObj  = array.getJSONObject(i);
                    result.append(jsonObj.getString("a") + "\n");
                }

                routingContext.response()
                        .putHeader("Connection", "keep-alive")
                        .putHeader("Content-Type", "text/plain;charset=UTF-8")
                        .end(result.toString());

            } catch (IOException e) {
                e.printStackTrace();
                routingContext.response()
                        .putHeader("Connection", "keep-alive")
                        .putHeader("Content-Type", "text/plain;charset=UTF-8")
                        .end(resultHeader);
            } catch (JSONException e) {
                e.printStackTrace();
                e.printStackTrace();
                routingContext.response()
                        .putHeader("Connection", "keep-alive")
                        .putHeader("Content-Type", "text/plain;charset=UTF-8")
                        .end(resultHeader);
            }
//        }


    }
}
