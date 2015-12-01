import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.ext.jdbc.JDBCClient;
import org.apache.hadoop.hbase.client.HTable;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Shimin Wang (andrewid: shiminw) on 15/10/16.
 *
 * This is a Singleton recordING configuration for our server
 */
public class ConfigSingleton {
    private static volatile ConfigSingleton instance = null;

    // Team information
    static final String TEAMID = "RXS";
    static final String TEAM_AWS_ACCOUNT_ID = "605262690154";

    static final String TEST_SERVER = "http://ec2-54-147-18-88.compute-1.amazonaws.com";



    // Which database we gonna use? "mysql" or "hbase"
    static final String DATABASE = "none";

    static Vertx vertx = null;
    static HttpClient httpClient = null;

    /** Config for mysql **/
    static final String mysqlDNS = "localhost";
//    static final String mysqlDNS = "54.174.202.217";
    //Q2
    static final String mysqlDBName = "twitter";

    static final String mysqlQ2TableName = "tweetscore";

    static final String mysqlQ3PosTableName = "q3_positive";
    static final String mysqlQ3NegTableName = "q3_negative";
    static final String mysqlQ3TableName = "q3_table";
    static final String mysqlQ3attrDate = "created_at";
    static final String mysqlQ3attrUid = "uid";
    static final String mysqlQ3attrContent = "content";
    static final String mysqlQ3attrTid = "tid";
    static final String mysqlQ3attrImpact = "impact";
    static final String mysqlQ3attrCentext = "content";


    // MySQL connection handler
    static Connection mysqlQ4conn = null;
    static Statement mysqlQ4stmt = null;

    static final String mysqlQ4MainTableName = "q4_table";
//    static final String mysqlQ4UidTableName = "";
//    static final String mysqlQ4attrDate = "created_at";
//    static final String mysqlQ4attrCount = "count";
//    static final String mysqlQ4attrHid = "hid";
//    static final String mysqlQ4attrSrctext = "text";
    static final String mysqlQ4attrHashTag = "hashtag";
    static final String mysqlQ4attrContent = "content";

    static final String mysqlUser = "root";
    static final String mysqlPass = "1314159";
    static JDBCClient mysqlClient = null;

    static final int MAX_MYSQL_CONNECTION = 900;
    //    static final int Q2_MAX_CONNECTION = MAX_MYSQL_CONNECTION;
    static final int Q2_MAX_CONNECTION = 10;
    static final int BLOCKING_QUEUE_SIZE = 10000/Q2_MAX_CONNECTION;


    /** Config for HBase **/
    static final String hbaseMasterIP = "52.91.37.53";
//    static final String hbaseMasterIP = "127.0.0.1";

    static final String hbaseQ2TableName = "q2_table";
    static final String hbaseQ2FamilyName = "TF";
    static final String hbaseQ2ColumnName = "content";

    static final String hbaseQ3TableName = "q3_table";
    static final String hbaseQ3FamilyName = "data";
    static final String hbaseQ3ColumnTid = "tid";
    static final String hbaseQ3ColumnImpact = "impact";
    static final String hbaseQ3ColumnCentext = "text";


    static final String hbaseQ4TableName = "q4_table";
    static final String hbaseQ4FamilyName = "data";
    static final String hbaseQ4ColumnName = "content";

    static HTable hbaseQ2Table = null;
    static HTable hbaseQ3Table = null;
    static HTable hbaseQ4Table = null;


    //Super Cache -- cache every request we got
    static HashMap<String, String> superCache = null;
    /** Config for q3 **/
    static final String[] slaveURL = {"54.152.183.54", "54.85.50.179", "52.91.173.165", "54.84.42.198",
                                    "52.23.162.85", "54.208.3.77", "54.208.6.173", "54.208.5.24"};
    static final int slaveNUM = 8;

    static final String q3FilePath = "/home/ubuntu/hashdata7";

    static HTreeMap<String, String> q3SuperCachePos = null;
    static HTreeMap<String, String> q3SuperCacheNeg = null;

    private ArrayList< HTreeMap<String, String> > loadQ3data() {

        DB db = DBMaker.memoryDB().make();
        HTreeMap<String, String> q3SuperCachePos = db.hashMapCreate("mapp")
                .keySerializer(new Serializer.CompressionWrapper(Serializer.STRING))
                .valueSerializer(new Serializer.CompressionWrapper(Serializer.STRING))
                .makeOrGet();

        HTreeMap<String, String> q3SuperCacheNeg = db.hashMapCreate("mapn")
                .keySerializer(new Serializer.CompressionWrapper(Serializer.STRING))
                .valueSerializer(new Serializer.CompressionWrapper(Serializer.STRING))
                .makeOrGet();

        ArrayList< HTreeMap<String, String> > result = new ArrayList< HTreeMap<String, String> >();

        System.out.println("\n\nLoading Q3 data...");
        try (BufferedReader br = new BufferedReader(new FileReader(q3FilePath))) {
            String line;
            int countLine = 0;
            while ((line = br.readLine()) != null) {
                // process the line.
                String [] s = line.split("\\t");
                String uid = s[0];

//                System.out.println(s[0]);
//                System.out.println(s[1]);
                JSONObject obj = new JSONObject(s[1]);


                q3SuperCachePos.put(uid, obj.getJSONArray("p").toString());
                q3SuperCacheNeg.put(uid, obj.getJSONArray("n").toString());

                countLine ++;
                if (countLine % 100000 == 0)
                    System.out.println(countLine + "lines loaded...");
            }
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        }

        System.out.println("Q3 data is loaded! \n\n");

        result.add(q3SuperCachePos);
        result.add(q3SuperCacheNeg);

        return result;
    }

    /** Config for q4 **/
    static final int Q4_INMEMORY_NUM = 3000000;
    static HashMap<String, String> q4SuperCache = null;

    static final String q4FilePath = "/home/ubuntu/q4/q4first3miilionData.tsv";
//    static final String q4FilePath = "/home/ubuntu/q4/q4_table.tsv";

    private HashMap<String, ArrayList<String>> loadQ4data() {
//        DB db = DBMaker.memoryDB().make();
//
//        HTreeMap<String, String> q4SuperCache = db.hashMapCreate("map")
//                .keySerializer(new Serializer.CompressionWrapper(Serializer.STRING))
//                .valueSerializer(new Serializer.CompressionWrapper(Serializer.STRING))
//                .makeOrGet();

//        HTreeMap<String, Long> map = db.hashMapCreate("map")
//                .keySerializer(Serializer.STRING)
//                .valueSerializer(Serializer.LONG)
//                .makeOrGet();

        HashMap<String, ArrayList<String>> q4SuperCache = new HashMap<String, ArrayList<String>>(Q4_INMEMORY_NUM, 1.f);

        System.out.println("\n\nLoading Q4 data...");
        try (BufferedReader br = new BufferedReader(new FileReader(q4FilePath))) {
            String line;
            int countLine = 0;
            while ((line = br.readLine()) != null) {
                // process the line.
                String [] s = line.split("\\t");
                String hashtag = s[0];

//                System.out.println(s[0]);
//                System.out.println(s[1]);
                JSONArray array = new JSONArray(s[1]);
                int arraylength = array.length();

                // update the cache
                ArrayList<String> result = new ArrayList<String>();
                for(int i = 0; i < arraylength ; i++){
                    JSONObject jsonObj  = array.getJSONObject(i);
                    result.add(jsonObj.getString("a") + "\n");
                }

                q4SuperCache.put(hashtag, result);

                countLine ++;
                if (countLine % 1000000 == 0)
                    System.out.println(countLine + "lines loaded...");
            }
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        }

        System.out.println("Q4 data is loaded! \n\n");

        return  q4SuperCache;
    }


    /** Config for q5 **/
    static ArrayList<TweetCound> q5SuperCache = null;

    public class TweetCound {
        long uid;
        int accumulateCount;

        public TweetCound(long uid, int accumulateCount) {
            this.uid = uid;
            this.accumulateCount = accumulateCount;
        }
    }

    static final String q5FilePath = "/home/ubuntu/q5data.tsv";
    static long maxUid;
    static long minUid;
    static int maxCount;
    static int q5DataNum;

    private ArrayList<TweetCound> loadQ5data() {
        ArrayList<TweetCound> q5SuperCache;
        q5SuperCache = new ArrayList<TweetCound>();
        q5SuperCache.add(new TweetCound(0, 0));

        System.out.println("\n\nLoading Q5 data...");
        try (BufferedReader br = new BufferedReader(new FileReader(q5FilePath))) {
            String line;
            int countLine = 0;
            while ((line = br.readLine()) != null) {
                // process the line.
                String [] s = line.split("\\t");
                q5SuperCache.add(new TweetCound(Long.parseLong(s[0]), Integer.parseInt(s[1])));
                countLine ++;
                if (countLine % 1000000 == 0)
                    System.out.println(countLine + "lines loaded...");
            }
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Q5 data is loaded! \n\n");

        q5DataNum = q5SuperCache.size();
        maxCount = q5SuperCache.get(q5DataNum-1).accumulateCount;
        maxUid = q5SuperCache.get(q5DataNum-1).uid;
        minUid = q5SuperCache.get(1).uid;

        return q5SuperCache;
    }



    AtomicInteger connectionCounter = null;

    ConfigSingleton() {
        connectionCounter = new AtomicInteger();

        vertx = Vertx.vertx();
        httpClient = vertx.createHttpClient();

        if(DATABASE.equals("none")){
            q5SuperCache = loadQ5data();
        } else if (DATABASE.equals("mysql")) {
            ArrayList< HTreeMap<String, String> > arr =  loadQ3data();
            q3SuperCachePos = arr.get(0);
            q3SuperCacheNeg = arr.get(1);

//            System.out.println("\n\n" + q3SuperCachePos.get("999964278") + "\n\n");

        }
//        q4SuperCache = loadQ4data();
    }


    public static ConfigSingleton getInstance() {
        if (instance == null) {
            synchronized (ConfigSingleton.class) {
                // Double check to avid thread race
                if (instance == null) {
                    instance = new ConfigSingleton();
                }
            }
        }
        return instance;
    }
}
