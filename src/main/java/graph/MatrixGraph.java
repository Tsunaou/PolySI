package graph;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Streams;
import com.google.common.graph.ElementOrder;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.Graph;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;

import org.apache.commons.lang3.tuple.Pair;
import org.roaringbitmap.RoaringBitmap;

import lombok.Getter;
import util.UnimplementedError;

public class MatrixGraph<T> implements MutableGraph<T> {
    @Getter
    private final ImmutableBiMap<T, Integer> nodeMap;
    private final RoaringBitmap adjacency[];
    // private final long adjacency[][];
    // private static final int LONG_BITS = 64;

    public MatrixGraph(Graph<T> graph) {
        Optional<List<T>> topoOrder = topoLogicalSort(graph);
        Function<Collection<T>, ImmutableBiMap<T, Integer>> toNodeMap = ((Function<Collection<T>, ImmutableBiMap<T, Integer>>) nodes ->
            Streams.mapWithIndex(nodes.stream(), (n, idx) -> Pair.of(n, (int) idx))
                .collect(ImmutableBiMap.toImmutableBiMap(Pair::getKey, Pair::getValue)));

        if (!topoOrder.isPresent()) {
            nodeMap = toNodeMap.apply(graph.nodes());
        } else {
            nodeMap = toNodeMap.apply(topoOrder.get());
        }

        adjacency = newMatrix(nodeMap.size());
        // adjacency = new long[i][(i + LONG_BITS - 1) / LONG_BITS];
        for (EndpointPair<T> e : graph.edges()) {
            putEdge(e.source(), e.target());
        }
    }

    public MatrixGraph(Graph<T> graph, ImmutableBiMap<T, Integer> nodeMap) {
        this.nodeMap = nodeMap;

        adjacency = newMatrix(nodeMap.size());
        for (EndpointPair<T> e : graph.edges()) {
            putEdge(e.source(), e.target());
        }
    }

    public static <T> MatrixGraph<T> ofNodes(MatrixGraph<T> graph) {
        return new MatrixGraph<>(graph.nodeMap);
    }

    private static RoaringBitmap[] newMatrix(int size) {
        RoaringBitmap[] m = new RoaringBitmap[size];
        for (int i = 0; i < size; i++) {
            m[i] = new RoaringBitmap();
        }
        return m;
    }

    private MatrixGraph(ImmutableBiMap<T, Integer> nodes) {
        nodeMap = nodes;
        adjacency = newMatrix(nodes.size());
        // adjacency = new long[nodes.size()][(nodes.size() + LONG_BITS - 1) /
        // LONG_BITS];
    }

    // private MatrixGraph(MatrixGraph<T> graph) {
    // nodeMap.putAll(graph.nodeMap);
    // adjacency = newMatrix(graph.adjacency.length);
    // // adjacency = new long[graph.adjacency.length][];
    // for (var i = 0; i < adjacency.length; i++) {
    // adjacency[i] = graph.adjacency[i].clone();
    // }
    // }

    private MatrixGraph<T> bfsWithNoCycle(List<Integer> topoOrder) {
        MatrixGraph<T> result = new MatrixGraph<T>(nodeMap);

        for (int i = topoOrder.size() - 1; i >= 0; i--) {
            Integer n = topoOrder.get(i);

            for (int j : successorIds(n).toArray()) {
                assert topoOrder.indexOf(j) > i;
                result.set(n, j);
                result.adjacency[n].or(result.adjacency[j]);
                // for (var k = 0; k < adjacency[0].length; k++) {
                // result.adjacency[n][k] |= result.adjacency[j][k];
                // }
            }
        }

        return result;
    }

    private MatrixGraph<T> allNodesBfs() {
        List<Integer> topoOrder = topoSortId().orElse(null);
        if (topoOrder != null) {
            return bfsWithNoCycle(topoOrder);
        }

        MatrixGraph<T> result = new MatrixGraph<>(this.nodeMap);
        Graph<Integer> graph = toSparseGraph();
        for (int i = 0; i < adjacency.length; i++) {
            ArrayDeque<Integer> q = new ArrayDeque<Integer>();

            q.add(i);
            while (!q.isEmpty()) {
                Integer j = q.pop();

                for (Integer k : graph.successors(j)) {
                    if (result.get(i, k)) {
                        continue;
                    }

                    result.set(i, k);
                    q.push(k);
                }
            }
        }

        return result;
    }

    public MatrixGraph<T> reachability() {
        MatrixGraph<T> result = allNodesBfs();
        for (int i = 0; i < result.nodeMap.size(); i++) {
            result.set(i, i);
        }

        return result;
    }

    private MatrixGraph<T> matrixProduct(MatrixGraph<T> other) {
        assert nodeMap.entrySet().equals(other.nodeMap.entrySet());

        MatrixGraph<T> result = new MatrixGraph<>(nodeMap);
        for (int i = 0; i < adjacency.length; i++) {
            for (Integer j : adjacency[i]) {
                result.adjacency[i].or(other.adjacency[j]);
            }
        }

        return result;
    }

    public MatrixGraph<T> composition(MatrixGraph<T> other) {
        return matrixProduct(other);
    }

    public MatrixGraph<T> union(MatrixGraph<T> other) {
        assert nodeMap.entrySet().equals(other.nodeMap.entrySet());

        MatrixGraph<T> result = new MatrixGraph<>(nodeMap);
        for (int i = 0; i < adjacency.length; i++) {
            result.adjacency[i] = RoaringBitmap.or(adjacency[i], other.adjacency[i]);
            // for (var j = 0; j < adjacency[0].length; j++)
            // result.adjacency[i][j] = adjacency[i][j] | other.adjacency[i][j];
            // }
        }

        return result;
    }

    private Optional<List<Integer>> topoSortId() {
        ArrayList<Integer> nodes = new ArrayList<Integer>();
        int[] inDegrees = new int[adjacency.length];

        for (int i = 0; i < adjacency.length; i++) {
            for (Integer j : adjacency[i]) {
                inDegrees[j]++;
            }
        }

        for (int i = 0; i < adjacency.length; i++) {
            if (inDegrees[i] == 0) {
                nodes.add(i);
            }
        }

        for (int i = 0; i < nodes.size(); i++) {
            successorIds(nodes.get(i)).forEach(n -> {
                if (--inDegrees[n] == 0) {
                    nodes.add(n);
                }
            });
        }

        return nodes.size() == adjacency.length ? Optional.of(nodes) : Optional.empty();
    }

    public Optional<List<T>> topologicalSort() {
        return topoSortId().map(o -> o.stream().map(n -> nodeMap.inverse().get(n)).collect(Collectors.toList()));
    }

    public boolean hasLoops() {
        return !topoSortId().isPresent();
    }

    private Graph<Integer> toSparseGraph() {
        MutableGraph<Integer> graph = GraphBuilder.directed().allowsSelfLoops(true).build();
        for (int i = 0; i < adjacency.length; i++) {
            graph.addNode(i);
        }

        for (int i = 0; i < adjacency.length; i++) {
            for (Integer j : adjacency[i]) {
                graph.putEdge(i, j);
            }
        }

        return graph;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append('\n');
        for (int i = 0; i < adjacency.length; i++) {
            for (int j = 0; j < adjacency.length; j++) {
                builder.append(get(i, j) ? 1 : 0);
                builder.append(' ');
            }
            builder.append('\n');
        }

        return builder.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MatrixGraph)) {
            return false;
        }

        MatrixGraph<T> g = (MatrixGraph<T>) obj;
        if (!nodeMap.equals(g.nodeMap)) {
            return false;
        }

        return Arrays.deepEquals(adjacency, g.adjacency);
    }

    @Override
    public Set<T> nodes() {
        return nodeMap.keySet();
    }

    @Override
    public Set<EndpointPair<T>> edges() {
        HashSet<EndpointPair<T>> result = new HashSet<EndpointPair<T>>();
        ImmutableBiMap<Integer, T> map = nodeMap.inverse();

        for (int i = 0; i < adjacency.length; i++) {
            for (Integer j : adjacency[i]) {
                result.add(EndpointPair.ordered(map.get(i), map.get(j)));
            }
        }

        return result;
    }

    @Override
    public boolean isDirected() {
        return true;
    }

    @Override
    public boolean allowsSelfLoops() {
        return true;
    }

    @Override
    public ElementOrder<T> nodeOrder() {
        return ElementOrder.unordered();
    }

    @Override
    public ElementOrder<T> incidentEdgeOrder() {
        return ElementOrder.unordered();
    }

    @Override
    public Set<T> adjacentNodes(T node) {
        throw new UnimplementedError();
    }

    @Override
    public Set<T> predecessors(T node) {
        throw new UnimplementedError();
    }

    @Override
    public Set<T> successors(T node) {
        ImmutableBiMap<Integer, T> inv = nodeMap.inverse();
        return successorIds(nodeMap.get(node)).mapToObj(inv::get).collect(Collectors.toSet());
    }

    @Override
    public Set<EndpointPair<T>> incidentEdges(T node) {
        throw new UnimplementedError();
    }

    @Override
    public int degree(T node) {
        throw new UnimplementedError();
    }

    @Override
    public int inDegree(T node) {
        return inDegree(nodeMap.get(node));
    }

    @Override
    public int outDegree(T node) {
        return outDegree(nodeMap.get(node));
    }

    @Override
    public boolean hasEdgeConnecting(T nodeU, T nodeV) {
        return get(nodeMap.get(nodeU), nodeMap.get(nodeV));
    }

    @Override
    public boolean hasEdgeConnecting(EndpointPair<T> endpoints) {
        return hasEdgeConnecting(endpoints.source(), endpoints.target());
    }

    public long nonZeroElements() {
        long n = 0;
        for (int i = 0; i < adjacency.length; i++) {
            n += outDegree(i);
        }

        return n;
    }

    private boolean get(int i, int j) {
        return adjacency[i].contains(j);
        // return (adjacency[i][j / LONG_BITS] & (1L << (j % LONG_BITS))) != 0;
    }

    private void set(int i, int j) {
        adjacency[i].add(j);
        // adjacency[i][j / LONG_BITS] |= (1L << (j % LONG_BITS));
    }

    private void clear(int i, int j) {
        adjacency[i].remove(j);
        // adjacency[i][j / LONG_BITS] &= ~(1L << (j % LONG_BITS));
    }

    private int inDegree(int n) {
        int inDegree = 0;
        for (int i = 0; i < adjacency.length; i++) {
            inDegree += get(i, n) ? 1 : 0;
        }

        return inDegree;
    }

    private int outDegree(int n) {
        return adjacency[n].getCardinality();
        // return
        // Arrays.stream(adjacency[n]).mapToInt(Long::bitCount).reduce(Integer::sum).orElse(0);
    }

    private IntStream successorIds(int n) {
        return adjacency[n].stream();
//        return IntStream.range(0, adjacency.length).filter(i -> get(n, i));
    }

    @Override
    public boolean addNode(T node) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean putEdge(T nodeU, T nodeV) {
        Integer i = nodeMap.get(nodeU);
        Integer j = nodeMap.get(nodeV);
        boolean hasEdge = get(i, j);
        set(i, j);
        return !hasEdge;
    }

    @Override
    public boolean putEdge(EndpointPair<T> endpoints) {
        return putEdge(endpoints.source(), endpoints.target());
    }

    @Override
    public boolean removeNode(T node) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeEdge(T nodeU, T nodeV) {
        Integer i = nodeMap.get(nodeU);
        Integer j = nodeMap.get(nodeV);
        boolean hasEdge = get(i, j);
        clear(i, j);
        return hasEdge;
    }

    @Override
    public boolean removeEdge(EndpointPair<T> endpoints) {
        return removeEdge(endpoints.source(), endpoints.target());
    }

    private static <T> Optional<List<T>> topoLogicalSort(Graph<T> graph) {
        ArrayList<T> list = new ArrayList<T>();
        Set<T> nodes = graph.nodes();
        HashMap<T, Integer> inDegrees = new HashMap<T, Integer>();

        for (T n : nodes) {
            int in = graph.inDegree(n);
            inDegrees.put(n, in);
            if (in == 0) {
                list.add(n);
            }
        }

        for (int i = 0; i < list.size(); i++) {
            for (T n : graph.successors(list.get(i))) {
                if (inDegrees.compute(n, (k, v) -> v - 1) == 0) {
                    list.add(n);
                }
            }
        }

        if (list.size() < nodes.size()) {
            return Optional.empty();
        }
        return Optional.of(list);
    }
}
