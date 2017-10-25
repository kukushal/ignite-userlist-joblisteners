package ignite.userlist.joblisteners;

import org.apache.ignite.*;
import org.jgrapht.experimental.dag.*;

public class CustomDirectedAcyclicGraph<V, E> extends DirectedAcyclicGraph<V, E> {
    private String targetVertex;

    public CustomDirectedAcyclicGraph(Class<? extends E> aClass) {
        super(aClass);
    }

    public void setCurrentVertex(String currentVertex) {
        Ignition.ignite().getOrCreateCache("cacheNameNew").put("CurrentVertex", currentVertex);
    }

    public void setTargetVertex(String targetVertex) {
        this.targetVertex = targetVertex;
    }

    public String getTargetVertex() {

        return targetVertex;
    }
}