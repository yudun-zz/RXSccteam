import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import sun.security.provider.certpath.Vertex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

/**
 * Created by yudun on 15/11/7.
 */
public class Q3 {
    final static String resultHeader = ConfigSingleton.TEAMID + "," +
            ConfigSingleton.TEAM_AWS_ACCOUNT_ID + "\n";

    static ConfigSingleton config = ConfigSingleton.getInstance();


    private static int getMinLargeEqual(JSONArray data, String target, int len) throws JSONException {
        int low = 0;
        int high = len - 1;

        while (high >= low) {

            int mid = (low + high) / 2;
            int compareResult = target.compareTo((data.getJSONObject(mid)).getString("d"));

//            System.out.println("\n\n" + (data.getJSONObject(mid)).getString("d") + "\n\n");

            if (compareResult < 0)         //update index of the
                high = mid - 1;           //right-most element considered

            else if (compareResult > 0)    //update index of
                low = mid + 1;            //left-most element considered

            else {
//                System.out.println( mid-1 + ":"+ data.get(mid - 1).accumulateCount + ", " +
//                        mid + ":"+ data.get(mid).accumulateCount);

                return mid;
            }
        }

//        System.out.println(data.get(low-1).accumulateCount + " " + data.get(low).accumulateCount +  " " + data.get(low).accumulateCount);

        if (high == -1 || target.compareTo( (data.getJSONObject(high)).getString("d") ) > 0)
            return high + 1;
        else
            return high;
    }


    static class q3Row {
        int imapct;
        String tid;
        String text;
        String date;
        public q3Row(int imapct, String tid, String text, String date) {
            this.imapct = imapct;
            this.tid = tid;
            this.text = text;
            this.date = date;
        }
    }

    static class myPosComparator implements Comparator<q3Row> {
        @Override
        public int compare(q3Row o1, q3Row o2) {
            if (o1.imapct == o2.imapct) {
                return o2.tid.compareTo(o1.tid);
            }
            else
                return o1.imapct - o2.imapct;
        }
    }

    static class myNegComparator implements Comparator<q3Row> {
        @Override
        public int compare(q3Row o1, q3Row o2) {
            if (o1.imapct == o2.imapct) {
                return o2.tid.compareTo(o1.tid);
            }
            else
                return o2.imapct - o1.imapct;
        }
    }

    static Comparator<q3Row> postiveComparator = new myPosComparator();
    static Comparator<q3Row> negativeComparator = new myNegComparator();

    private static String getQ3Response(String url) {
//        System.out.println(url);

        try {
            URL obj = new URL(url);

            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
//            con.setConnectTimeout(CONNECT_TIMEOUT);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream(),"utf-8"));

            byte[] bytes = IOUtils.toByteArray(in);
            in.close();
            return (new String(bytes)).toString();

        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return "";
    }

    private static int hashQ3(String userid) {
        long uid = Long.parseLong(userid);
        return (int)(uid % config.slaveNUM);
    }

    public static void getResponseByHash(String userid, String query, RoutingContext routingContext){
        String url = "http://" + config.slaveURL[hashQ3(userid)] + "/q3?" + query;

//        HttpClient httpClient = config.vertx.createHttpClient();

//        config.httpClient.getNow(8080, config.slaveURL[hashQ3(userid)], "/q3?" + query, new Handler<HttpClientResponse>() {
//            @Override
//            public void handle(HttpClientResponse httpClientResponse) {
//                httpClientResponse.bodyHandler(new Handler<Buffer>() {
//                   @Override
//                   public void handle(Buffer respStr) {
//                       routingContext.response()
//                               .putHeader("Connection", "keep-alive")
//                               .putHeader("Content-Type", "text/plain;charset=UTF-8")
//                               .end(respStr.toString());
//                   }
//               });
//
//            }
//        });

        routingContext.response()
                .putHeader("Connection", "keep-alive")
                .putHeader("Content-Type", "text/plain;charset=UTF-8")
                .end(getQ3Response(url));

    }

    private static StringBuilder getContent(JSONArray plist, JSONArray nlist, String start_date, String end_date, String n)
            throws JSONException {

        int plen = plist.length();
        int nlen = nlist.length();

        int pstartIndex = getMinLargeEqual(plist, start_date, plen);
        int nstartIndex = getMinLargeEqual(nlist, start_date, nlen);

        PriorityQueue<q3Row> qpositive = new PriorityQueue<q3Row>(1, postiveComparator);
        PriorityQueue<q3Row> qnegative = new PriorityQueue<q3Row>(1, negativeComparator);

        int num = Integer.parseInt(n);


        for (int i = pstartIndex; i < plen; i++) {

            JSONObject item = plist.getJSONObject(i);
            String date = item.getString("d");
            int impact = item.getInt("i");

            if (date.compareTo(end_date) > 0)
                break;

            if (qpositive.size() < num) {
                String tid = item.getString("t");
                String centext = item.getString("a");
                qpositive.add(new q3Row(impact, tid, centext, date));
            }
            else {
                if (qpositive.peek().imapct < impact) {
                    qpositive.poll();
                    String tid = item.getString("t");
                    String centext = item.getString("a");
                    qpositive.add(new q3Row(impact, tid, centext, date));
                }
                else if (qpositive.peek().imapct == impact) {
                    String tid = item.getString("t");
                    if (qpositive.peek().tid.compareTo(tid) > 0 ){
                        qpositive.poll();
                        String centext = item.getString("a");
                        qpositive.add(new q3Row(impact, tid, centext, date));
                    }
                }
            }
        }

        for (int i = nstartIndex; i < nlen; i++) {

            JSONObject item = nlist.getJSONObject(i);
            String date = item.getString("d");
            int impact = item.getInt("i");

            if (date.compareTo(end_date) > 0)
                break;

            if (qnegative.size() < num) {
                String tid = item.getString("t");
                String centext = item.getString("a");
                qnegative.add(new q3Row(impact, tid, centext, date));
            }
            else {
                if (qnegative.peek().imapct > impact) {
                    qnegative.poll();
                    String tid = item.getString("t");
                    String centext = item.getString("a");
                    qnegative.add(new q3Row(impact, tid, centext, date));
                }
                else if (qnegative.peek().imapct == impact) {
                    String tid = item.getString("t");
                    if (qnegative.peek().tid.compareTo(tid) > 0 ){
                        qnegative.poll();
                        String centext = item.getString("a");
                        qnegative.add(new q3Row(impact, tid, centext, date));
                    }
                }
            }
        }


        LinkedList<q3Row> postiveArray = new LinkedList<q3Row>();
        LinkedList<q3Row> negativeArray = new LinkedList<q3Row>();

        while (qpositive.size() > 0) {
            postiveArray.add(0, qpositive.poll());
        }
        while (qnegative.size() > 0) {
            negativeArray.add(0, qnegative.poll());
        }

        StringBuilder value = new StringBuilder("Positive Tweets\n");
        for (q3Row r : postiveArray) {
            value.append(r.date+","+r.imapct+","+r.tid+","+ StringEscapeUtils.unescapeJava(r.text)+"\n");
        }

        value.append("\nNegative Tweets\n");
        for (q3Row r : negativeArray) {
            value.append(r.date+","+r.imapct+","+r.tid+","+ StringEscapeUtils.unescapeJava(r.text)+"\n");
        }

        return value;
    }


    public static void getResponseFromCache(String start_date, String end_date, String userid, String n,
                                            RoutingContext routingContext){

        StringBuilder result = new StringBuilder(resultHeader);

        if(config.q3SuperCachePos.containsKey(userid)){
            try {
                JSONArray plist = new JSONArray(config.q3SuperCachePos.get(userid));

                JSONArray nlist = new JSONArray(config.q3SuperCacheNeg.get(userid));

//                System.out.println("\n\n plist length =" + plist.length() + "\n\n");
                StringBuilder value = getContent(plist, nlist, start_date, end_date, n);
                result.append(value);
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        routingContext.response()
                .putHeader("Connection", "keep-alive")
                .putHeader("Content-Type", "text/plain;charset=UTF-8")
                .end(result.toString());
    }


    public static void lookupMysql(SQLConnection connection,
                               String start_date, String end_date, String userid, String n,
                               RoutingContext routingContext){

        connection.query("SELECT "+config.mysqlQ3attrContent +
                " FROM " + config.mysqlQ3TableName +
                " WHERE " + config.mysqlQ3attrUid+ "=\'" + userid + "\'", res2 -> {

            StringBuilder result = new StringBuilder(resultHeader);

            if (res2.succeeded()) {
//                        System.out.println("start_date=" + start_date + ":" + "end_date=" + end_date);
                List<JsonObject> rows = res2.result().getRows();
                StringBuilder value = new StringBuilder("");

                for (JsonObject row : rows) {
                    value.append(row.getString(config.mysqlQ3attrContent) );
                }

                try {

                    JSONObject obj = new JSONObject(value.toString());

                    JSONArray plist = obj.getJSONArray("p");
                    JSONArray nlist = obj.getJSONArray("n");

                    value = getContent(plist, nlist, start_date, end_date, n);
                    result.append(value);

                } catch (JSONException e) {
                    e.printStackTrace();
                }

                routingContext.response()
                        .putHeader("Connection", "keep-alive")
                        .putHeader("Content-Type", "text/plain;charset=UTF-8")
                        .end(result.toString());
            }
        });
    }

//    public static void lookupMysql(SQLConnection connection,
//                                   String start_date, String end_date, String userid, String n,
//                                   RoutingContext routingContext){
//
////        String key = userid + "+" + tweet_time.replace(" ", "+");
////        if (config.superCache.containsKey(key)){
////            StringBuilder result = new StringBuilder(resultHeader);
////            result.append(config.superCache.get(key));
////            routingContext.response()
////                    .putHeader("Connection", "keep-alive")
////                    .putHeader("Content-Type", "text/plain;charset=UTF-8")
////                    .end(result.toString());
////        }
////        else {
//
////        System.out.println("\n\nSELECT "+config.mysqlQ3attrDate+", "+config.mysqlQ3attrImpact+
////                ", "+config.mysqlQ3attrTid+", "+ config.mysqlQ3attrUid +
////                ", " + config.mysqlQ3attrCentext +
////                " FROM " + config.mysqlQ3PosTableName +
////                " WHERE "+config.mysqlQ3attrUid+"=\'" + userid + "\'" +
////                " AND DATE("+config.mysqlQ3attrDate+")>=\'" + start_date + "\'" +
////                " AND DATE("+config.mysqlQ3attrDate+")<=\'" + end_date + "\'" +
////                " ORDER BY "+config.mysqlQ3attrImpact+" DESC, "+config.mysqlQ3attrTid + " ASC " +
////                " LIMIT " + n + "\n\n");
//
//
//            connection.query("SELECT DATE_FORMAT("+ config.mysqlQ3attrDate +",'%Y-%m-%d') as date, "+config.mysqlQ3attrImpact+
//                                    ", "+config.mysqlQ3attrTid+", "+ config.mysqlQ3attrUid +
//                                    ", " + config.mysqlQ3attrCentext +
//                    " FROM " + config.mysqlQ3PosTableName +
//                    " WHERE "+config.mysqlQ3attrUid+"=\'" + userid + "\'" +
//                    " AND DATE("+config.mysqlQ3attrDate+")>=\'" + start_date + "\'" +
//                    " AND DATE("+config.mysqlQ3attrDate+")<=\'" + end_date + "\'" +
//                    " ORDER BY "+config.mysqlQ3attrImpact+" DESC, "+config.mysqlQ3attrTid + " ASC " +
//                    " LIMIT " + n, res2 -> {
//
//                StringBuilder result = new StringBuilder(resultHeader);
//
//                if (res2.succeeded()) {
////                        System.out.println("start_date=" + start_date + ":" + "end_date=" + end_date);
//                    List<JsonObject> posrows = res2.result().getRows();
//
//
//                    StringBuilder posvalue = new StringBuilder("Positive Tweets\n");
//
//                    for (JsonObject row : posrows) {
//                        posvalue.append(row.getString("date") + "," +
//                                row.getInteger(config.mysqlQ3attrImpact) + "," +
//                                row.getString(config.mysqlQ3attrTid) + "," +
//                                new JsonObject(row.getString(config.mysqlQ3attrCentext)).getString("text") + "\n");
//                    }
//
//                    result.append(posvalue);
//
//                    connection.query( "SELECT DATE_FORMAT("+ config.mysqlQ3attrDate +",'%Y-%m-%d') as date, "+ config.mysqlQ3attrImpact +
//                            ", "+config.mysqlQ3attrTid+", "+ config.mysqlQ3attrUid +
//                            ", " + config.mysqlQ3attrCentext +
//                            " FROM " + config.mysqlQ3NegTableName +
//                            " WHERE "+config.mysqlQ3attrUid+"=\'" + userid + "\'" +
//                            " AND DATE("+config.mysqlQ3attrDate+")>=\'" + start_date + "\'" +
//                            " AND DATE("+config.mysqlQ3attrDate+")<=\'" + end_date + "\'" +
//                            " ORDER BY "+config.mysqlQ3attrImpact+" ASC, "+config.mysqlQ3attrTid + " ASC " +
//                            " LIMIT " + n, res3 -> {
//
//                        List<JsonObject> negrows = res3.result().getRows();
//
//
//                        StringBuilder negvalue = new StringBuilder("\nNegative Tweets\n");
//
//                        for (JsonObject row : negrows) {
//                            negvalue.append(row.getString("date") + "," +
//                                    row.getInteger(config.mysqlQ3attrImpact) + "," +
//                                    row.getString(config.mysqlQ3attrTid) + "," +
//                                    new JsonObject(row.getString(config.mysqlQ3attrCentext)).getString("text") + "\n");
//
//                        }
//
////                    config.superCache.put(key, value.toString());
//                        result.append(negvalue);
//                        routingContext.response()
//                                .putHeader("Connection", "keep-alive")
//                                .putHeader("Content-Type", "text/plain;charset=UTF-8")
//                                .end(result.toString());
//                    });
//                }
//
//            });
////        }
//    }

    public static void lookupHbase(String start_date, String end_date, String userid, String n,
                                   RoutingContext routingContext){
        StringBuilder result = new StringBuilder(resultHeader);

//        String key = userid + "+" + start_date;
//        if (config.superCache.containsKey(key)){
//            result.append(config.superCache.get(key));
//            routingContext.response()
//                    .putHeader("Connection", "keep-alive")
//                    .putHeader("Content-Type", "text/plain;charset=UTF-8")
//                    .end(result.toString());
//        }
//        else{

            try {

                FilterList filterList = new FilterList();

//                filterList.addFilter(new RowFilter(
//                        CompareFilter.CompareOp.EQUAL,
//                        new BinaryPrefixComparator(Bytes.toBytes(userid + "+" + start_date )) ));
//
//                filterList.addFilter(new RowFilter(
//                        CompareFilter.CompareOp.LESS_OR_EQUAL,
//                        new BinaryPrefixComparator(Bytes.toBytes(userid + "+" + end_date )) ));


                Scan scan = new Scan(Bytes.toBytes(userid + "+" + start_date + "+00:00:00"),
                        Bytes.toBytes(userid + "+" + end_date + "+23:59:59"));

                scan.addColumn(Bytes.toBytes(config.hbaseQ3FamilyName), Bytes.toBytes(config.hbaseQ3ColumnTid));
                scan.addColumn(Bytes.toBytes(config.hbaseQ3FamilyName), Bytes.toBytes(config.hbaseQ3ColumnImpact));
                scan.addColumn(Bytes.toBytes(config.hbaseQ3FamilyName), Bytes.toBytes(config.hbaseQ3ColumnCentext));


                ResultScanner scanner = config.hbaseQ3Table.getScanner(scan);

                PriorityQueue<q3Row> qpositive = new PriorityQueue<q3Row>(1, postiveComparator);
                PriorityQueue<q3Row> qnegative = new PriorityQueue<q3Row>(1, negativeComparator);

                int num = Integer.parseInt(n);

                for (Result rs : scanner) {
                    String date = Bytes.toString(rs.getRow()).split("\\+")[1];

//                    System.out.println(date);
//                    System.out.println(rs);
//                    System.out.println(Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
//                            Bytes.toBytes(config.hbaseQ3ColumnImpact))));
//
//                    System.out.println(Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
//                            Bytes.toBytes(config.hbaseQ3ColumnTid))));
//
//                    System.out.println(Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
//                            Bytes.toBytes(config.hbaseQ3ColumnCentext))));


                    int impact = Integer.parseInt(Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                            Bytes.toBytes(config.hbaseQ3ColumnImpact))));

                    if (impact > 0 ) {
                        if (qpositive.size() < num) {
                            String tid = Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                                    Bytes.toBytes(config.hbaseQ3ColumnTid)));
                            String centext = Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                                    Bytes.toBytes(config.hbaseQ3ColumnCentext)));
                            qpositive.add(new q3Row(impact, tid, centext, date));
                        }
                        else {
                            if (qpositive.peek().imapct < impact) {
                                qpositive.poll();
                                String tid = Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                                        Bytes.toBytes(config.hbaseQ3ColumnTid)));
                                String centext = Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                                        Bytes.toBytes(config.hbaseQ3ColumnCentext)));
                                qpositive.add(new q3Row(impact, tid, centext, date));
                            }
                            else if (qpositive.peek().imapct == impact) {
                                String tid = Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                                        Bytes.toBytes(config.hbaseQ3ColumnTid)));
                                if (qpositive.peek().tid.compareTo(tid) > 0 ){
                                    qpositive.poll();
                                    String centext = Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                                            Bytes.toBytes(config.hbaseQ3ColumnCentext)));
                                    qpositive.add(new q3Row(impact, tid, centext, date));
                                }
                            }
                        }
                    }
                    else {
                        if (qnegative.size() < num) {
                            String tid = Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                                    Bytes.toBytes(config.hbaseQ3ColumnTid)));
                            String centext = Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                                    Bytes.toBytes(config.hbaseQ3ColumnCentext)));
                            qnegative.add(new q3Row(impact, tid, centext, date));
                        }
                        else {
                            if (qnegative.peek().imapct > impact) {
                                qnegative.poll();
                                String tid = Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                                        Bytes.toBytes(config.hbaseQ3ColumnTid)));
                                String centext = Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                                        Bytes.toBytes(config.hbaseQ3ColumnCentext)));
                                qnegative.add(new q3Row(impact, tid, centext, date));
                            }
                            else if (qnegative.peek().imapct == impact) {
                                String tid = Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                                        Bytes.toBytes(config.hbaseQ3ColumnTid)));
                                if (qnegative.peek().tid.compareTo(tid) > 0 ){
                                    qnegative.poll();
                                    String centext = Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                                            Bytes.toBytes(config.hbaseQ3ColumnCentext)));
                                    qnegative.add(new q3Row(impact, tid, centext, date));
                                }
                            }
                        }
                    }
                }


                LinkedList<q3Row> postiveArray = new LinkedList<q3Row>();
                LinkedList<q3Row> negativeArray = new LinkedList<q3Row>();

                while (qpositive.size() > 0) {
                    postiveArray.add(0, qpositive.poll());
                }
                while (qnegative.size() > 0) {
                    negativeArray.add(0, qnegative.poll());
                }

                StringBuilder value = null;

                value = new StringBuilder("Positive Tweets\n");
                for (q3Row r : postiveArray) {
                    value.append(r.date+","+r.imapct+","+r.tid+","+ StringEscapeUtils.unescapeJava(r.text)+"\n");
                }

                value.append("\nNegative Tweets\n");
                for (q3Row r : negativeArray) {
                    value.append(r.date+","+r.imapct+","+r.tid+","+ StringEscapeUtils.unescapeJava(r.text)+"\n");
                }


//                value = Bytes.toString(res.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
//                        Bytes.toBytes(config.))) + "\n";

//                config.superCache.put(key, value);

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
//        }

    }

}
