package ignite.userlist.joblisteners;

import org.apache.ignite.*;

public class Server {
    public static void main(String[] args) throws IgniteException {
        Ignition.start("ignite-server.xml");
    }
}
