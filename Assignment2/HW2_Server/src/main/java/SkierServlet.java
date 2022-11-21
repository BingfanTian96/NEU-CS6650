import Models.LiftData;
import Models.LiftRide;
import RMQPool.RMQChannelFactory;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.annotation.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

@WebServlet(name = "SkierServlet", value = "/SkierServlet")
public class SkierServlet extends HttpServlet {

    private static final int DAY_MIN = 1;
    private static final int DAY_MAX = 366;

    private final static String QUEUE_NAME = "SkierPostQueue";

    private Gson gson  = new Gson();

    private ObjectPool<Channel> pool;
    private ConnectionFactory factory;



    @Override
    public void init() {
        try {
            this.factory = new ConnectionFactory();
            factory.setHost("35.91.107.69");
            factory.setPort(5672);
            factory.setUsername("guest");
            factory.setPassword("guest");
            this.pool = new GenericObjectPool<Channel>(new RMQChannelFactory(factory.newConnection()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        String urlPath = request.getPathInfo();

        // check we have a URL!
        if (urlPath == null || urlPath.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().write("missing parameters");
            return;
        }

        String[] urlParts = urlPath.split("/");

        if (!isUrlValid(urlParts)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("incorrect parameters");
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write("It works!");
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res)
            throws ServletException, IOException {
        processRequest(req, res);
    }

    private void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        String urlPath = request.getPathInfo();

        // check we have a URL!
        if (urlPath == null || urlPath.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().write("missing parameters");
            return;
        }

        String[] urlParts = urlPath.split("/");

        if (!isUrlValid(urlParts)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getOutputStream().println("incorrect parameters");
        } else {
            StringBuilder sb = new StringBuilder();
            String s;
            while ((s = request.getReader().readLine()) != null) {
                sb.append(s);
            }
            LiftRide liftRide = gson.fromJson(sb.toString(), LiftRide.class);
            if(isBodyValid(liftRide)) {
                JsonObject liftInfo = new JsonObject();
                liftInfo.addProperty("resortId", Integer.valueOf(urlParts[1]));
                liftInfo.addProperty("seasonId", Integer.valueOf(urlParts[3]));
                liftInfo.addProperty("dayId", Integer.valueOf(urlParts[5]));
                liftInfo.addProperty("skierId", Integer.valueOf(urlParts[7]));
                liftInfo.addProperty("time", liftRide.getTime());
                liftInfo.addProperty("liftId", liftRide.getLiftID());
                if (sendToQueue(liftInfo)) {
                    response.setStatus(HttpServletResponse.SC_CREATED);
                    response.getWriter().write(gson.toJson("success"));
                } else {
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    response.getWriter().write("failed");
                }
            } else {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().write("incorrect request body");
            }
        }
    }

    private boolean isUrlValid(String[] urlPath) {
        if(urlPath.length != 8) {
            return false;
        } else {
            return isNumeric(urlPath[1]) && urlPath[2].equals("seasons") &&
                    isNumeric(urlPath[3]) && urlPath[3].length() == 4 && urlPath[4].equals("days") &&
                    isNumeric(urlPath[5]) &&
                    Integer.parseInt(urlPath[5]) >= DAY_MIN &&
                    Integer.parseInt(urlPath[5]) <= DAY_MAX &&
                    urlPath[6].equals("skiers") && isNumeric(urlPath[7]);
        }

    }

    private boolean isNumeric(String s) {
        if(s == null || s.equals("")) return false;
        try {
            Integer.parseInt(s);
            return true;
        } catch (NumberFormatException ignored) { }
        return false;
    }

    private boolean isBodyValid(LiftRide liftRide) {
        if(liftRide.getTime() == null || liftRide.getLiftID() == null)
            return false;
        return true;
    }

    private boolean sendToQueue(JsonObject liftInfo) {
        Channel channel = null;
        try {
            channel = pool.borrowObject();
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN,
                    liftInfo.toString().getBytes(StandardCharsets.UTF_8));
            return true;
        } catch (Exception e) {
            System.out.println("Unable to borrow channel from pool" + e.toString());
            return false;
        } finally {
            if (channel != null) {
                try {
                    pool.returnObject(channel);
                } catch (Exception e) {
                    System.out.println("Cannot return channel");
                }
            }
        }
    }
}
