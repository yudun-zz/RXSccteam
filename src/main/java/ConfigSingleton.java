import io.vertx.ext.jdbc.JDBCClient;
import org.apache.hadoop.hbase.client.HTable;
import org.codehaus.jettison.json.JSONArray;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
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
    static final String DATABASE = "hbase";



    /** Config for mysql **/
    static final String mysqlDNS = "localhost";
//    static final String mysqlDNS = "54.174.202.217";
    //Q2
    static final String mysqlDBName = "twitter";

    static final String mysqlQ2TableName = "q2_table";

    static final String mysqlQ3PosTableName = "q3_positive";
    static final String mysqlQ3NegTableName = "q3_negative";
    static final String mysqlQ3attrDate = "created_at";
    static final String mysqlQ3attrUid = "uid";
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
    static final String mysqlQ4attrHashTag = "hash";
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
    static HashMap<String, JSONArray> q4SuperCache = null;

    AtomicInteger connectionCounter = null;

    ConfigSingleton() {
        connectionCounter = new AtomicInteger();
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
