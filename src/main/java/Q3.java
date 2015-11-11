import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * Created by yudun on 15/11/7.
 */
public class Q3 {
    final static String resultHeader = ConfigSingleton.TEAMID + "," +
            ConfigSingleton.TEAM_AWS_ACCOUNT_ID + "\n";

    static ConfigSingleton config = ConfigSingleton.getInstance();


    public static void lookupMysql(SQLConnection connection,
                                   String start_date, String end_date, String userid, String n,
                                   RoutingContext routingContext){

//        String key = userid + "+" + tweet_time.replace(" ", "+");
//        if (config.superCache.containsKey(key)){
//            StringBuilder result = new StringBuilder(resultHeader);
//            result.append(config.superCache.get(key));
//            routingContext.response()
//                    .putHeader("Connection", "keep-alive")
//                    .putHeader("Content-Type", "text/plain;charset=UTF-8")
//                    .end(result.toString());
//        }
//        else {

//        System.out.println("\n\nSELECT "+config.mysqlQ3attrDate+", "+config.mysqlQ3attrImpact+
//                ", "+config.mysqlQ3attrTid+", "+ config.mysqlQ3attrUid +
//                ", " + config.mysqlQ3attrCentext +
//                " FROM " + config.mysqlQ3PosTableName +
//                " WHERE "+config.mysqlQ3attrUid+"=\'" + userid + "\'" +
//                " AND DATE("+config.mysqlQ3attrDate+")>=\'" + start_date + "\'" +
//                " AND DATE("+config.mysqlQ3attrDate+")<=\'" + end_date + "\'" +
//                " ORDER BY "+config.mysqlQ3attrImpact+" DESC, "+config.mysqlQ3attrTid + " ASC " +
//                " LIMIT " + n + "\n\n");


            connection.query("SELECT DATE_FORMAT("+ config.mysqlQ3attrDate +",'%Y-%m-%d') as date, "+config.mysqlQ3attrImpact+
                                    ", "+config.mysqlQ3attrTid+", "+ config.mysqlQ3attrUid +
                                    ", " + config.mysqlQ3attrCentext +
                    " FROM " + config.mysqlQ3PosTableName +
                    " WHERE "+config.mysqlQ3attrUid+"=\'" + userid + "\'" +
                    " AND DATE("+config.mysqlQ3attrDate+")>=\'" + start_date + "\'" +
                    " AND DATE("+config.mysqlQ3attrDate+")<=\'" + end_date + "\'" +
                    " ORDER BY "+config.mysqlQ3attrImpact+" DESC, "+config.mysqlQ3attrTid + " ASC " +
                    " LIMIT " + n, res2 -> {

                StringBuilder result = new StringBuilder(resultHeader);

                if (res2.succeeded()) {
//                        System.out.println("start_date=" + start_date + ":" + "end_date=" + end_date);
                    List<JsonObject> posrows = res2.result().getRows();


                    StringBuilder posvalue = new StringBuilder("Positive Tweets\n");

                    for (JsonObject row : posrows) {
                        posvalue.append(row.getString("date") + "," +
                                row.getInteger(config.mysqlQ3attrImpact) + "," +
                                row.getString(config.mysqlQ3attrTid) + "," +
                                new JsonObject(row.getString(config.mysqlQ3attrCentext)).getString("text") + "\n");
                    }

                    result.append(posvalue);

                    connection.query( "SELECT DATE_FORMAT("+ config.mysqlQ3attrDate +",'%Y-%m-%d') as date, "+ config.mysqlQ3attrImpact +
                            ", "+config.mysqlQ3attrTid+", "+ config.mysqlQ3attrUid +
                            ", " + config.mysqlQ3attrCentext +
                            " FROM " + config.mysqlQ3NegTableName +
                            " WHERE "+config.mysqlQ3attrUid+"=\'" + userid + "\'" +
                            " AND DATE("+config.mysqlQ3attrDate+")>=\'" + start_date + "\'" +
                            " AND DATE("+config.mysqlQ3attrDate+")<=\'" + end_date + "\'" +
                            " ORDER BY "+config.mysqlQ3attrImpact+" ASC, "+config.mysqlQ3attrTid + " ASC " +
                            " LIMIT " + n, res3 -> {

                        List<JsonObject> negrows = res3.result().getRows();


                        StringBuilder negvalue = new StringBuilder("\nNegative Tweets\n");

                        for (JsonObject row : negrows) {
                            negvalue.append(row.getString("date") + "," +
                                    row.getInteger(config.mysqlQ3attrImpact) + "," +
                                    row.getString(config.mysqlQ3attrTid) + "," +
                                    new JsonObject(row.getString(config.mysqlQ3attrCentext)).getString("text") + "\n");

                        }

//                    config.superCache.put(key, value.toString());
                        result.append(negvalue);
                        routingContext.response()
                                .putHeader("Connection", "keep-alive")
                                .putHeader("Content-Type", "text/plain;charset=UTF-8")
                                .end(result.toString());
                    });

                }

            });
//        }
    }

    static class q3HbaseRow {
        int imapct;
        String tid;
        String text;
        String date;
        public q3HbaseRow(int imapct, String tid, String text, String date) {
            this.imapct = imapct;
            this.tid = tid;
            this.text = text;
            this.date = date;
        }
    }

    static class myPosComparator implements Comparator<q3HbaseRow> {
        @Override
        public int compare(q3HbaseRow o1, q3HbaseRow o2) {
            if (o1.imapct == o2.imapct) {
                return o2.tid.compareTo(o1.tid);
            }
            else
                return o1.imapct - o2.imapct;
        }
    }

    static class myNegComparator implements Comparator<q3HbaseRow> {
        @Override
        public int compare(q3HbaseRow o1, q3HbaseRow o2) {
            if (o1.imapct == o2.imapct) {
                return o2.tid.compareTo(o1.tid);
            }
            else
                return o2.imapct - o1.imapct;
        }
    }

    static Comparator<q3HbaseRow> postiveComparator = new myPosComparator();
    static Comparator<q3HbaseRow> negativeComparator = new myNegComparator();

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

                PriorityQueue<q3HbaseRow> qpositive = new PriorityQueue<q3HbaseRow>(1, postiveComparator);
                PriorityQueue<q3HbaseRow> qnegative = new PriorityQueue<q3HbaseRow>(1, negativeComparator);

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
                            qpositive.add(new q3HbaseRow(impact, tid, centext, date));
                        }
                        else {
                            if (qpositive.peek().imapct < impact) {
                                qpositive.poll();
                                String tid = Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                                        Bytes.toBytes(config.hbaseQ3ColumnTid)));
                                String centext = Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                                        Bytes.toBytes(config.hbaseQ3ColumnCentext)));
                                qpositive.add(new q3HbaseRow(impact, tid, centext, date));
                            }
                            else if (qpositive.peek().imapct == impact) {
                                String tid = Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                                        Bytes.toBytes(config.hbaseQ3ColumnTid)));
                                if (qpositive.peek().tid.compareTo(tid) > 0 ){
                                    qpositive.poll();
                                    String centext = Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                                            Bytes.toBytes(config.hbaseQ3ColumnCentext)));
                                    qpositive.add(new q3HbaseRow(impact, tid, centext, date));
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
                            qnegative.add(new q3HbaseRow(impact, tid, centext, date));
                        }
                        else {
                            if (qnegative.peek().imapct > impact) {
                                qnegative.poll();
                                String tid = Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                                        Bytes.toBytes(config.hbaseQ3ColumnTid)));
                                String centext = Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                                        Bytes.toBytes(config.hbaseQ3ColumnCentext)));
                                qnegative.add(new q3HbaseRow(impact, tid, centext, date));
                            }
                            else if (qnegative.peek().imapct == impact) {
                                String tid = Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                                        Bytes.toBytes(config.hbaseQ3ColumnTid)));
                                if (qnegative.peek().tid.compareTo(tid) > 0 ){
                                    qnegative.poll();
                                    String centext = Bytes.toString(rs.getValue(Bytes.toBytes(config.hbaseQ3FamilyName),
                                            Bytes.toBytes(config.hbaseQ3ColumnCentext)));
                                    qnegative.add(new q3HbaseRow(impact, tid, centext, date));
                                }
                            }
                        }
                    }
                }


                LinkedList<q3HbaseRow> postiveArray = new LinkedList<q3HbaseRow>();
                LinkedList<q3HbaseRow> negativeArray = new LinkedList<q3HbaseRow>();

                while (qpositive.size() > 0) {
                    postiveArray.add(0, qpositive.poll());
                }
                while (qnegative.size() > 0) {
                    negativeArray.add(0, qnegative.poll());
                }

                StringBuilder value = null;

                value = new StringBuilder("Positive Tweets\n");
                for (q3HbaseRow r : postiveArray) {
                    value.append(r.date+","+r.imapct+","+r.tid+","+ StringEscapeUtils.unescapeJava(r.text)+"\n");
                }

                value.append("\nNegative Tweets\n");
                for (q3HbaseRow r : negativeArray) {
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
