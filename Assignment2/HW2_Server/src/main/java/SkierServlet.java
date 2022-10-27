import Models.LiftRide;
import RMQPool.RMQChannelFactory;
import RMQPool.RMQChannelPool;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
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



    @Override
    public void init() {
        this.pool = new GenericObjectPool<Channel>(new RMQChannelFactory());
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
//                JsonObject liftInfo = new JsonObject();
//                liftInfo.addProperty("resortID", Integer.valueOf(urlParts[1]));
//                liftInfo.addProperty("seasonID", Integer.valueOf(urlParts[3]));
//                liftInfo.addProperty("dayID", Integer.valueOf(urlParts[5]));
//                liftInfo.addProperty("skierId", Integer.valueOf(urlParts[7]));
//                liftInfo.addProperty("time", liftRide.getTime());
//                liftInfo.addProperty("liftId", liftRide.getLiftID());
//                String liftInfo = "resortID" + urlParts[1] + "seasonID" + urlParts[3] + "dayID" + urlParts[5] +

                try {
                    Channel channel = pool.borrowObject();
                    channel.queueDeclare(QUEUE_NAME, true, false, false, null);
//                    channel.basicPublish("", QUEUE_NAME, null,
//                            liftInfo.toString().getBytes("UTF-8"));
                    channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN,
                            gson.toJson(liftRide).getBytes(StandardCharsets.UTF_8));
                } catch (Exception e) {
                    throw new RuntimeException("Unable to borrow channel from pool" + e.toString());
                }
            } else {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().write("incorrect request body");
            }
        }
    }

    private boolean isUrlValid(String[] urlPath) {
//        if(urlPath.length != 8) {
//            return false;
//        } else {
//            return isNumeric(urlPath[1]) && urlPath[2].equals("seasons") &&
//                    isNumeric(urlPath[3]) && urlPath[3].length() == 4 && urlPath[4].equals("days") &&
//                    isNumeric(urlPath[5]) &&
//                    Integer.parseInt(urlPath[5]) >= DAY_MIN &&
//                    Integer.parseInt(urlPath[5]) <= DAY_MAX &&
//                    urlPath[6].equals("skiers") && isNumeric(urlPath[7]);
//        }
        return true;
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
//        if(liftRide.getTime() == null || liftRide.getLiftID() == null)
//            return false;
        return true;
    }
}
