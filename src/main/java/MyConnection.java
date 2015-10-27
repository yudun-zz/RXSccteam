import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLConnection;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by yudun on 15/10/25.
 */
public class MyConnection {
    private static ConfigSingleton config = ConfigSingleton.getInstance();

    static String resultHeader = ConfigSingleton.TEAMID + "," +
            ConfigSingleton.TEAM_AWS_ACCOUNT_ID + "\n";

    private SQLConnection connection = null;
    private BlockingQueue<Q2Request> bq = null;

    public MyConnection (SQLConnection connection, ArrayList<BlockingQueue<Q2Request>> globalQueueArray){
        this.connection = connection;
        bq = new ArrayBlockingQueue<Q2Request>(config.BLOCKING_QUEUE_SIZE);

        // update the global blockingQueue array
        globalQueueArray.add(bq);
    }

    public void start(){
        while(true) {
            try {
                final Q2Request re = bq.take();

                connection.query("SELECT tid, score, content" +
                        " FROM " + config.Q2TableName +
                        " WHERE created_at=\'" + re.tweet_time + "\'" +
                        " AND uid=\'" + re.userid + "\'", res2 -> {

                    StringBuilder result = new StringBuilder(resultHeader);

                    if (res2.succeeded()) {
    //                        System.out.println("uid=" + userid + ":" + "time=" + tweet_time);
                        List<JsonObject> rows = res2.result().getRows();

                        //Sort numerically by Tweet ID in ascending order
                        rows.sort((r1, r2) ->
                                ((new BigInteger(r1.getString("tid")))
                                        .compareTo(new BigInteger(r2.getString("tid")))));

    //                        System.out.println(rows);
                        for (JsonObject row : rows) {
                            result.append(row.getString("tid") + ":" +
                                    row.getInteger("score") + ":" +
                                    row.getString("content") + "\n");

                        }

                    }

                    re.routingContext.response()
                            .putHeader("Connection", "keep-alive")
                            .putHeader("Content-Type", "text/plain;charset=UTF-8")
                            .end(result.toString());

    //                    connection.close();
                });

            } catch (InterruptedException e) {
                e.printStackTrace();
                continue;
            }

        }
    }

}
