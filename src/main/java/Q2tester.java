/**
 * Created by yudun on 15/10/23.
 */
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLConnection;
import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

public class Q2tester {

    static ConfigSingleton config = ConfigSingleton.getInstance();
    private static final int CONNECT_TIMEOUT = 2000;

    // send a GET request to the url and return the content as a string
    private static String getReferenceResult(String userid, String tweet_time) {
        String url = config.TEST_SERVER + "/q2?userid=" + userid + "&tweet_time="+tweet_time;
//        System.out.println(url);

        try {
            URL obj = new URL(url);

            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setConnectTimeout(CONNECT_TIMEOUT);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream(),"utf-8"));

//            int inputLine;
//            StringBuffer response = new StringBuffer();
//
//            while ((inputLine = (in.read())) != -1) {
//                response.append((char)inputLine);
//            }
//
//            in.close();

            byte[] bytes = IOUtils.toByteArray(in);
//            System.out.println(new String(bytes));
            return (new String(bytes)).toString().split("\n", 2)[1];
        } catch (Exception e ){

        }

        return "";
    }

    private static void getMyResultFromMysql(String userid, String tweet_time){

        config.mysqlClient.getConnection(res -> {

            if (res.succeeded()) {
                SQLConnection connection = res.result();

                System.out.println("connection# = " + config.connectionCounter.getAndIncrement());

                connection.query("SELECT tid, score, content" +
                        " FROM "+ config.Q2TableName +
                        " WHERE created_at=\'" + tweet_time + "\'" +
                        " AND uid=\'" + userid +"\'", res2 -> {

                    StringBuilder result = new StringBuilder();

                    if (res2.succeeded()) {

                        List<JsonObject> rows = res2.result().getRows();

                        //Sort numerically by Tweet ID in ascending order
                        rows.sort((r1, r2) ->
                                ( (new BigInteger(r1.getString("tid")))
                                        .compareTo(new BigInteger(r2.getString("tid"))) ));

                        for (JsonObject row : rows) {
                                result.append(row.getString("tid") + ":" +
                                        row.getInteger("score") + ":" +
                                        /** The stupid reference server replace all ";" with "\n"!!!!!!
                                         so we do it the same way **/
                                        row.getString("content").replace(";", "\n") + "\n" );
                        }

                        String correctResult = getReferenceResult(userid, tweet_time);
                        comprare(result.toString(), correctResult, userid, tweet_time);
                    }

//                    connection.close();
                });

            } else {
                // Failed to get connection - deal with it
            }
        });

    }

    private static void comprare(String myResult, String correctResult, String uid, String time){
        if (!myResult.equals(correctResult)){
//            System.out.println(myResult);
//            System.out.println(correctResult);
//            System.out.println("/q2?userid=" + uid + "&tweet_time="+time.replace(' ','+'));
//            System.out.println("----------------------------------------------");
        }
//        else {
//            System.out.println("match!:");
//            System.out.println(myResult);
//            System.out.println(correctResult);
//            System.out.println("----------------------------------------------");
//        }
    }

    public static void start() {
        config.mysqlClient.getConnection(res -> {
            if (res.succeeded()) {
                SQLConnection connection = res.result();

                connection.query("SELECT uid, DATE_FORMAT(created_at, \"%Y-%m-%d+%T\") as date " +
                        " FROM " + config.Q2TableName, res2 -> {

                    StringBuilder result = new StringBuilder();

                    if (res2.succeeded()) {
                        List<JsonObject> rows = res2.result().getRows();

                        System.out.println("----------------------------------------------");
                        System.out.println("Begin Test! Error url will be shown below:");

                        for (JsonObject row : rows) {
                            getMyResultFromMysql(row.getString("uid"), row.getString("date"));
                        }

                    }


                });

            } else {
                // Failed to get connection - deal with it
            }
        });
    }

}
