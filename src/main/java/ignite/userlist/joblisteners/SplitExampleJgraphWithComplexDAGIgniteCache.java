package ignite.userlist.joblisteners;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.jetbrains.annotations.*;

import java.text.*;
import java.util.*;

@ComputeTaskSessionFullSupport
public class SplitExampleJgraphWithComplexDAGIgniteCache
    extends ComputeTaskSplitAdapter<CustomDirectedAcyclicGraph<String, DefaultEdge>, Integer> {

    // Auto-injected task session.
    @TaskSessionResource
    private ComputeTaskSession ses;

    private static final Random random = new Random();
    static int noOftasksExecutedSuccess = 0;

    SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss:SSS");

    @Override protected Collection<? extends ComputeJob> split(
        int clusterSize,
        CustomDirectedAcyclicGraph<String, DefaultEdge> graph) {

        Collection<ComputeJob> jobs = new LinkedList<>();

        IgniteCache<String, Object> cacheUp = Ignition.ignite().getOrCreateCache("cacheNameNew");
        ses.addAttributeListener((key, val) -> {
            if ("COMPLETE".compareTo(key.toString()) == 0) {
                nextTaskToExecute(graph, cacheUp);
            }
        }, false);

        String task = null;
        if (cacheUp.get("CurrentVertex") != null)
            task = (String)cacheUp.get("CurrentVertex");

        for (DefaultEdge outgoingEdge : graph.outgoingEdgesOf(task)) {
            String sourceVertex = graph.getEdgeSource(outgoingEdge);
            String targetVertex = graph.getEdgeTarget(outgoingEdge);
            graph.setTargetVertex(targetVertex);
            executingJobsBuilt(graph, jobs);
        }

        if (task != null && graph.outgoingEdgesOf(task).size() == 0) {
            if (cacheUp.get(task) != null && (Boolean)cacheUp.get(task)) {
                String targetVertex = setNextVertexInCache(graph, cacheUp);
                graph.setTargetVertex(targetVertex);
                nextTaskToExecute(graph, cacheUp);
            }
            else {
                System.out.println("else parttttt");
            }
        }

        return jobs;
    }

    private void nextTaskToExecute(CustomDirectedAcyclicGraph<String, DefaultEdge> graph,
        IgniteCache<String, Object> cacheUp) {
        Ignite ignite = Ignition.ignite();
        if (cacheUp.get("NextVertex") != null) {
            String processingVertex = (String)cacheUp.get("NextVertex");
            if (processingVertex != null && areParentVerticesProcessed(graph,
                processingVertex, cacheUp)) {
                cacheUp.put("CurrentVertex", processingVertex);
                // Execute task on the cluster and wait for its completion.

                ignite.compute().execute(SplitExampleJgraphWithComplexDAGIgniteCache.class, graph);
            }
        }
    }

    private void executingJobsBuilt(
        CustomDirectedAcyclicGraph<String, DefaultEdge> graph,
        Collection<ComputeJob> jobs) {

        String targetVertex = graph.getTargetVertex();
        IgniteCache<String, Object> cacheNew = Ignition.ignite().getOrCreateCache("cacheNameNew");

        if (targetVertex != null && !cacheNew.containsKey(targetVertex)) {
            jobs.add(new ComputeJobAdapter() {
                // Auto-injected job context.
                @JobContextResource
                private ComputeJobContext jobCtx;

                @Nullable @Override public Object execute() {
                    int duration1 = 8000 + random.nextInt(100);
                    SimpleDateFormat dateFormatNew = new SimpleDateFormat("HH:mm:ss:SSS");
                    String task = (String)targetVertex;
                    try {
                        Thread.sleep(duration1);

                        System.out.println("**************************** executed the job **********" +
                            task + " * *****" + dateFormatNew.format(new Date()));
                        cacheNew.put(task, true);
                    }
                    catch (Exception e1) {
                        e1.printStackTrace();
                    }
                    ses.setAttribute("NEXTVERTEX", setNextVertexInCache(graph, cacheNew));
                    ses.setAttribute("COMPLETE", duration1);
                    return duration1;
                }
            });
        }
    }

    private String setNextVertexInCache(
        CustomDirectedAcyclicGraph<String, DefaultEdge> graph,
        IgniteCache<String, Object> cache) {

        String task = null;
        Set<String> dagSourceVertex = graph.vertexSet();
        Iterator itr = dagSourceVertex.iterator();
        while (itr.hasNext()) {
            task = (String)itr.next();
            if (cache.get("CurrentVertex") != null &&
                !task.equalsIgnoreCase((String)cache.get("CurrentVertex")))
                continue;
            else {
                task = (String)itr.next();
                cache.put("NextVertex", task);
                break;
            }
        }
        return task;
    }

    private Boolean areParentVerticesProcessed(
        CustomDirectedAcyclicGraph<String, DefaultEdge> graph,
        String task,
        IgniteCache<String, Object> cache) {

        Boolean processed = false;
        for (DefaultEdge incomingEdge : graph.incomingEdgesOf(task)) {
            //graph.outgoingEdgesOf(dagSourceVertex)
            String sourceVertex = graph.getEdgeSource(incomingEdge);
            String targetVertex = graph.getEdgeTarget(incomingEdge);
            if (cache != null && cache.get(sourceVertex) != null) {
                processed = true;
            }
        }

        return processed;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Integer reduce(List<ComputeJobResult>
        results) {
        int sum = 0;

        for (ComputeJobResult res : results) {
            sum += res.<Integer>getData();
        }

        return sum;
    }
}
