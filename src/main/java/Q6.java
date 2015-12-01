import io.vertx.ext.web.RoutingContext;

/**
 * Created by tengjiaochen on 11/30/15.
 */
public class Q6 {
    final static String resultHeader = ConfigSingleton.TEAMID + "," +
            ConfigSingleton.TEAM_AWS_ACCOUNT_ID + "\n";


    public static void getResponse(String tid, String seq, String opt, String tweetid, String tag,
                                   RoutingContext routingContext) {

    }
}
