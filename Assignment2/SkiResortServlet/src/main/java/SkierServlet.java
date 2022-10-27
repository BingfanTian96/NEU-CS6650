import Models.LiftData;
import Models.LiftRide;
import Models.Status;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.servlet.*;
import javax.servlet.http.*;
import java.io.IOException;

public class SkierServlet extends HttpServlet {
    private static final int DAY_MIN = 1;
    private static final int DAY_MAX = 366;

    private final static String QUEUE_NAME = "SkierServletPostQueue";

    private Gson gson  = new Gson();

    private GenericObjectPool<Channel> pool;



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
            LiftRide liftRide = (LiftRide) gson.fromJson(sb.toString(), LiftRide.class);
            if (liftRide.getLiftID() != null && liftRide.getTime() != null) {
                response.setStatus(HttpServletResponse.SC_CREATED);
                response.getOutputStream().println(gson.toJson(liftRide));
                response.getOutputStream().println("It works!");
                response.getOutputStream().flush();
            } else {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getOutputStream().println(gson.toJson(liftRide));
                response.getOutputStream().println("Bad request!");
                response.getOutputStream().flush();
            }
        }
    }
}
