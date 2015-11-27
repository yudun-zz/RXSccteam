import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.RoutingContext;

/**
 * Created by Shimin Wang on 11/26/15.
 */
public class Q5 {
    final static String resultHeader = ConfigSingleton.TEAMID + "," +
            ConfigSingleton.TEAM_AWS_ACCOUNT_ID + "\n";

    static ConfigSingleton config = ConfigSingleton.getInstance();
    public static void getCount(long userid_min, long userid_max,
                                   RoutingContext routingContext){

    }
}
