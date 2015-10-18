import io.vertx.core.AbstractVerticle;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;


/**
 * Created by yudun on 15/10/17.
 */
public class CCTeam extends AbstractVerticle {

    Router router = Router.router(vertx);

    public void Q1(Route routerQ1){
        routerQ1.handler(routingContext -> {
            String key = routingContext.request().getParam("key");
            String message = routingContext.request().getParam("message");

            String result = Q1.decrypt(key, message);

            routingContext.response().end(result);
        });
    }

    public void Q2(Route routerQ2){
        routerQ2.handler(routingContext -> {

            routingContext.response().end("q2");
        });
    }

    public static void main(String[] args) {
        Runner.runExample(CCTeam.class);
    }

    @Override
    public void start() throws Exception {

        Route routerQ1 = router.route("/q1");
        Q1(routerQ1);

        Route routerQ2 = router.route("/q2");
        Q2(routerQ2);

        vertx.createHttpServer().requestHandler(router::accept).listen(8080);
    }

}

