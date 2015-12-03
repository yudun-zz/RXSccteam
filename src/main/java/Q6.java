import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.RoutingContext;

import java.io.*;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

/**
 * Created by Shimin Wang on 11/30/15.
 */
public class Q6 {
    final static String resultHeader = ConfigSingleton.TEAMID + "," +
            ConfigSingleton.TEAM_AWS_ACCOUNT_ID + "\n";

    final static String responseStartAndEnd = resultHeader + "0\n";

    static ConfigSingleton config = ConfigSingleton.getInstance();
    static char[] buf = new char[16384];

    public static String getTextFromMysql(String url){
        try {
            URL obj = new URL(url);

            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
//            con.setConnectTimeout(CONNECT_TIMEOUT);

//            BufferedReader in = new BufferedReader(
//                    new InputStreamReader(con.getInputStream(),"utf-8"));
            con.setUseCaches(false);
            final InputStream is = con.getInputStream();
            final Reader reader = new InputStreamReader(is);
            int read;
            final StringBuilder sb = new StringBuilder();
            while((read = reader.read(buf)) > 0) {
                sb.append(buf, 0, read);
            }

//            byte[] bytes = IOUtils.toByteArray(in);
//            in.close();
//            return (new String(bytes)).toString();
            return sb.toString();

        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return "";
    }

    public static void getResponse(String tid, String seq, String opt, String tweetid, String tag,
                                   RoutingContext routingContext) {


        if (opt.equals("s") || opt.equals("e")) {
            routingContext.response()
                    .putHeader("Connection", "keep-alive")
                    .putHeader("Content-Type", "text/plain;charset=UTF-8")
                    .end(responseStartAndEnd);
            if (opt.equals("e"))
                    config.q6SuperCache.remove("tid");
        }
        else {
            //backup
//            StringBuilder response = new StringBuilder(resultHeader);
//            routingContext.response()
//                    .putHeader("Connection", "keep-alive")
//                    .putHeader("Content-Type", "text/plain;charset=UTF-8")
//                    .end(response.append(tag + "\n").toString());


            config.vertx.executeBlocking(future -> {
                StringBuilder response = new StringBuilder(resultHeader);

                if (opt.equals("a")) {
                    routingContext.response()
                            .putHeader("Connection", "keep-alive")
                            .putHeader("Content-Type", "text/plain;charset=UTF-8")
                            .end(response.append(tag + "\n").toString());
                }

                ConfigSingleton.Q6Request thisr = new  ConfigSingleton.Q6Request(ConfigSingleton.Q6_APPEND, tweetid, tag);

                if (config.q6SuperCache.containsKey(tid)) {
                    config.q6SuperCache.put(tid, new ConfigSingleton.Q6Request[5]);
                    config.q6LockMap.put(tid, new Object());
                }

                ConfigSingleton.Q6Request[] arr = config.q6SuperCache.get(tid);
                int seqIdx = Integer.parseInt(seq);
                arr[seqIdx] = thisr;

                if (opt.equals("r")){
                    String t = "";
                    for (int i = 0; i < seqIdx-1; i++) {
                        synchronized (config.q6LockMap.get(tid)){
                            while (arr[i] == null)
                                try {
                                    config.q6LockMap.get(tid).wait(3000);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }

                            ConfigSingleton.Q6Request r = arr[i];
                            if (r.OPT == config.Q6_APPEND && r.tweetid.equals(tweetid))
                                t = r.tag;
                        }
                    }

                    routingContext.response()
                            .putHeader("Connection", "keep-alive")
                            .putHeader("Content-Type", "text/plain;charset=UTF-8")
                            .end(response.append(getTextFromMysql(config.slaveURL[config.q6SlaveIndex++] + "/q6?tid=" + tweetid) + t + "\n").toString());
                }

            }, false, result -> {});

        }

    }

    /** open a new connection for each request **/
    public static void lookupMysql(String tid, RoutingContext routingContext){

        config.mysqlClient.getConnection(res -> {
            if (res.succeeded()) {
                final SQLConnection connection = res.result();
                connection.query("SELECT content" +
                        " FROM " + config.mysqlQ2TableName +
                        " WHERE tid=\'" + tid + "\'", res2 -> {

                    StringBuilder result = new StringBuilder();

                    if (res2.succeeded()) {
//                        System.out.println("uid=" + userid + ":" + "time=" + tweet_time);
                        List<JsonObject> rows = res2.result().getRows();

//                        System.out.println(rows);
                        for (JsonObject row : rows) {
                            result.append(row.getString("content"));
                            break;
                        }

                    }

                    routingContext.response()
                            .putHeader("Connection", "keep-alive")
                            .putHeader("Content-Type", "text/plain;charset=UTF-8")
                            .end(result.toString());

                });
            } else {
                // Failed to get connection - deal with it
//                routingContext.response()
//                        .putHeader("Connection", "keep-alive")
//                        .putHeader("Content-Type", "text/plain;charset=UTF-8")
//                        .end(resultHeader);
            }

            res.result().close();
        });
    }
}
