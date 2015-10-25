import io.vertx.ext.web.RoutingContext;

/**
 * Created by yudun on 15/10/25.
 */
public class Q2Request {
    String userid;
    String tweet_time;
    RoutingContext routingContext;

    Q2Request(String userid, String tweet_time, RoutingContext routingContext){
        this.userid = userid;
        this.tweet_time = tweet_time;
        this.routingContext = routingContext;
    }
}
