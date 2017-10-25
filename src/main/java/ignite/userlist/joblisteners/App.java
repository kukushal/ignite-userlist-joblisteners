package ignite.userlist.joblisteners;

import org.apache.ignite.*;

public class App {

    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("ignite-client.xml")) {
            int res = ignite.compute().execute(SplitExampleJgraphWithComplexDAGIgniteCache.class, buildGraph());

            System.out.format("Total duration: %.2f sec.\n", res / 1000.0);
        }
    }

    private static CustomDirectedAcyclicGraph<String, DefaultEdge> buildGraph() {

        CustomDirectedAcyclicGraph<String, DefaultEdge> graph = new CustomDirectedAcyclicGraph<>(DefaultEdge.class);

        String root = "root";
        String a = "1";
        String b = "2";
        String c = "3";
        String d = "4";
        String e = "5";
        String f = "6";
        String g = "7";
        String h = "8";
        String i = "9";
        String j = "10";
        String k = "11";
        String l = "12";
        String m = "13";
        String n = "14";

        graph.addVertex(root);
        graph.addVertex(a);
        graph.addVertex(l);
        graph.addVertex(k);

        graph.addVertex(b);
        graph.addVertex(c);
        graph.addVertex(m);

        graph.addVertex(i);
        graph.addVertex(h);
        graph.addVertex(g);

        graph.addVertex(f);
        graph.addVertex(e);
        graph.addVertex(d);
        graph.addVertex(n);
        graph.addVertex(j);

        graph.addEdge(root, l);
        graph.addEdge(root, k);

        graph.addEdge(a, b);
        graph.addEdge(a, c);
        graph.addEdge(l, m);

        graph.addEdge(b, i);
        graph.addEdge(b, h);
        graph.addEdge(b, g);

        graph.addEdge(c, f);
        graph.addEdge(c, e);
        graph.addEdge(c, d);

        graph.addEdge(m, d);
        graph.addEdge(m, n);

        graph.addEdge(i, j);

        graph.setCurrentVertex(root);

        return graph;
    }
}
