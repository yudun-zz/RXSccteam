import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.RoutingContext;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

/**
 * Created by yudun on 15/10/23.
 */
public class Q2 {
    final static String resultHeader = ConfigSingleton.TEAMID + "," +
            ConfigSingleton.TEAM_AWS_ACCOUNT_ID + "\n";

    static ConfigSingleton config = ConfigSingleton.getInstance();

    public static void lookupMysql(SQLConnection connection, String userid, String tweet_time, RoutingContext routingContext){
        String key = userid + "+" + tweet_time.replace(" ", "+");

        if (config.superCache.containsKey(key)){
            StringBuilder result = new StringBuilder(resultHeader);
            result.append(config.superCache.get(key));
            routingContext.response()
                    .putHeader("Connection", "keep-alive")
                    .putHeader("Content-Type", "text/plain;charset=UTF-8")
                    .end(result.toString());
        }
        else {
            connection.query("SELECT tid, score, content" +
                    " FROM " + config.mysqlQ2TableName +
                    " WHERE uid=\'" + userid + "\'" +
                    " AND created_at=\'" + tweet_time + "\'" , res2 -> {

                StringBuilder result = new StringBuilder(resultHeader);

                if (res2.succeeded()) {
//                        System.out.println("uid=" + userid + ":" + "time=" + tweet_time);
                    List<JsonObject> rows = res2.result().getRows();

                    //Sort numerically by Tweet ID in ascending order
                    rows.sort((r1, r2) ->
                            ((new BigInteger(r1.getString("tid")))
                                    .compareTo(new BigInteger(r2.getString("tid")))));

//                        System.out.println(rows);

                    StringBuilder value = new StringBuilder("");

                    for (JsonObject row : rows) {
                        value.append(row.getString("tid") + ":" +
                                row.getInteger("score") + ":" +
                                row.getString("content") + "\n");

                    }

                    config.superCache.put(key, value.toString());
                    result.append(value);
                }

                routingContext.response()
                        .putHeader("Connection", "keep-alive")
                        .putHeader("Content-Type", "text/plain;charset=UTF-8")
                        .end(result.toString());

            });
        }
    }


    public static void lookupHbase(String userid, String tweet_time, RoutingContext routingContext){
        StringBuilder result = new StringBuilder(resultHeader);

        String key = userid + "+" + tweet_time.replace(" ", "+");

        if (config.superCache.containsKey(key)){
            result.append(config.superCache.get(key));
            routingContext.response()
                    .putHeader("Connection", "keep-alive")
                    .putHeader("Content-Type", "text/plain;charset=UTF-8")
                    .end(result.toString());
        }
        else{
            Get query = new Get(Bytes.toBytes(key));
            query.addFamily(Bytes.toBytes(config.hbaseQ2FamilyName));

            try {
                query.setMaxVersions(3);
                Result res = config.hbaseQ2Table.get(query);

                String value = Bytes.toString(res.getValue(Bytes.toBytes(config.hbaseQ2FamilyName),
                        Bytes.toBytes(config.hbaseQ2ColumnName))) + "\n";

                config.superCache.put(key, value);

                result.append(value);
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
            }
        }


    }
}
