package verifier;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.MutableValueGraph;
import com.google.common.graph.ValueGraph;
import com.google.common.graph.ValueGraphBuilder;

import history.Session;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import graph.Edge;
import graph.EdgeType;
import graph.MatrixGraph;
import history.Event;
import history.History;
import history.Transaction;
import history.Event.EventType;
import monosat.Lit;
import monosat.Logic;
import monosat.Solver;

class Utils {
    static <KeyType, ValueType> boolean verifyInternalConsistency(History<KeyType, ValueType> history) {
        HashMap<Pair<KeyType, ValueType>, Pair<Event<KeyType, ValueType>, Integer>> writes = new HashMap<Pair<KeyType, ValueType>, Pair<Event<KeyType, ValueType>, Integer>>();
        HashMap<Pair<Transaction<KeyType, ValueType>, KeyType>, ArrayList<Integer>> txnWrites = new HashMap<Pair<Transaction<KeyType, ValueType>, KeyType>, ArrayList<Integer>>();
        Function<EventType, Stream<Pair<Integer, Event<KeyType, ValueType>>>> getEvents = ((Function<Event.EventType, Stream<Pair<Integer, Event<KeyType, ValueType>>>>) type -> history
                .getTransactions().stream().flatMap(txn -> {
                List<Event<KeyType, ValueType>> events = txn.getEvents();
                    return IntStream.range(0, events.size()).mapToObj(i -> Pair.of(i, events.get(i)))
                            .filter(p -> p.getRight().getType() == type);
                }));

        getEvents.apply(Event.EventType.WRITE).forEach(p -> {
            Integer i = p.getLeft();
            Event<KeyType, ValueType> ev = p.getRight();
            writes.put(Pair.of(ev.getKey(), ev.getValue()), Pair.of(ev, i));
            txnWrites.computeIfAbsent(Pair.of(ev.getTransaction(), ev.getKey()), k -> new ArrayList()).add(i);
        });

        for (Pair<Integer, Event<KeyType, ValueType>> p : getEvents.apply(Event.EventType.READ).collect(Collectors.toList())) {
            Integer i = p.getLeft();
            Event<KeyType, ValueType> ev = p.getRight();
            Pair<Event<KeyType, ValueType>, Integer> writeEv = writes.get(Pair.of(ev.getKey(), ev.getValue()));

            if (writeEv == null) {
                System.err.printf("%s has no corresponding write\n", ev);
                return false;
            }

            ArrayList<Integer> writeIndices = txnWrites.get(Pair.of(writeEv.getLeft().getTransaction(), writeEv.getLeft().getKey()));
            int j = Collections.binarySearch(writeIndices, writeEv.getRight());

            if (writeEv.getLeft().getTransaction() == ev.getTransaction()) {
                if (j != writeIndices.size() - 1 && writeIndices.get(j + 1) < i) {
                    System.err.printf("%s not reading from latest write: %s\n", ev, writeEv.getLeft());
                    return false;
                } else if (writeEv.getRight() > i) {
                    System.err.printf("%s reads from a write after it: %s\n", ev, writeEv.getLeft());
                    return false;
                }
            } else if (j != writeIndices.size() - 1) {
                System.err.printf("%s not reading from latest write: %s\n", ev, writeEv.getLeft());
                return false;
            }
        }
        return true;
    }

    /**
     * Collect unknown edges
     *
     * @param graphA       graph A containing known and unknown edges
     * @param graphB       graph B containing known and unknown edges
     * @param reachability known reachable node pairs. Edges that connect reachable
     *                     pairs are not collected
     * @param solver       SAT solver
     */
    static <KeyType, ValueType> List<Triple<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>, Lit>> getUnknownEdges(
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> graphA,
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> graphB,
            MatrixGraph<Transaction<KeyType, ValueType>> reachability, Solver solver) {
        ArrayList<Triple<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>, Lit>> edges = new ArrayList<Triple<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>, Lit>>();

        for (Transaction<KeyType, ValueType> p : graphA.nodes()) {
            for (Transaction<KeyType, ValueType> n : graphA.successors(p)) {
                Collection<Lit> predEdges = graphA.edgeValue(p, n).get();

                if (p == n || !reachability.hasEdgeConnecting(p, n)) {
                    predEdges.forEach(e -> edges.add(Triple.of(p, n, e)));
                }

                List<Transaction<KeyType, ValueType>> txns = graphB.successors(n).stream()
                        .filter(t -> p == t || !reachability.hasEdgeConnecting(p, t))
                        .collect(Collectors.toList());

                for (Transaction<KeyType, ValueType> s : txns) {
                    Collection<Lit> succEdges = graphB.edgeValue(n, s).get();
                    predEdges.forEach(e1 -> succEdges.forEach(e2 -> {
                        Lit lit = Logic.and(e1, e2);
                        solver.setDecisionLiteral(lit, false);
                        edges.add(Triple.of(p, s, lit));
                    }));
                }
            }
        }

        return edges;
    }

    /**
     * Collect known edges in A union C
     *
     * @param graphA known graph A
     * @param graphB known graph B
     * @param AC     the graph containing the edges to collect
     */
    static <KeyType, ValueType> List<Triple<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>, Lit>> getKnownEdges(
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> graphA,
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> graphB,
            MatrixGraph<Transaction<KeyType, ValueType>> AC) {
        return AC.edges().stream().map(e -> {
            Transaction<KeyType, ValueType> n = e.source();
            Transaction<KeyType, ValueType> m = e.target();
            Function<Optional<Collection<Lit>>, Lit> firstEdge = ((Function<Optional<Collection<Lit>>, Lit>) c -> c.get().iterator().next());

            if (graphA.hasEdgeConnecting(n, m)) {
                return Triple.of(n, m, firstEdge.apply(graphA.edgeValue(n, m)));
            }

            Transaction<KeyType, ValueType> middle = Sets.intersection(graphA.successors(n), graphB.predecessors(m)).iterator().next();
            return Triple.of(n, m, Logic.and(firstEdge.apply(graphA.edgeValue(n, middle)),
                    firstEdge.apply(graphB.edgeValue(middle, m))));
        }).collect(Collectors.toList());
    }

    static <KeyType, ValueType> Map<Transaction<KeyType, ValueType>, Integer> getOrderInSession(
            History<KeyType, ValueType> history) {
        // @formatter:off
        return history.getSessions().stream()
                .flatMap(s -> Streams.zip(
                    s.getTransactions().stream(),
                    IntStream.range(0, s.getTransactions().size()).boxed(),
                    Pair::of))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        // @formatter:on
    }

    static <KeyType, ValueType> MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> createEmptyGraph(
            History<KeyType, ValueType> history) {
        MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> g = ValueGraphBuilder.directed()
                .allowsSelfLoops(true).build();

        history.getTransactions().forEach(g::addNode);
        return g;
    }

    static <KeyType, ValueType> void addEdge(MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> g,
            Transaction<KeyType, ValueType> src, Transaction<KeyType, ValueType> dst, Lit lit) {
        if (!g.hasEdgeConnecting(src, dst)) {
            g.putEdgeValue(src, dst, new ArrayList<>());
        }
        g.edgeValue(src, dst).get().add(lit);
    }

    /*
     * Delete edges in a way that preserves reachability
     */
    static <KeyType, ValueType> MatrixGraph<Transaction<KeyType, ValueType>> reduceEdges(
            MatrixGraph<Transaction<KeyType, ValueType>> graph,
            Map<Transaction<KeyType, ValueType>, Integer> orderInSession) {
        System.err.printf("Before: %d edges\n", graph.edges().size());
        MatrixGraph<Transaction<KeyType, ValueType>> newGraph = MatrixGraph.ofNodes(graph);

        for (Transaction<KeyType, ValueType> n : graph.nodes()) {
            Set<Transaction<KeyType, ValueType>> succ = graph.successors(n);
            // @formatter:off
            Map<Session<KeyType, ValueType>, Transaction<KeyType, ValueType>> firstInSession = succ.stream()
                .collect(Collectors.toMap(
                    m -> m.getSession(),
                    Function.identity(),
                    (p, q) -> orderInSession.get(p)
                        < orderInSession.get(q) ? p : q));

            firstInSession.values().forEach(m -> newGraph.putEdge(n, m));

            succ.stream()
                .filter(m -> m.getSession() == n.getSession()
                        && orderInSession.get(m) == orderInSession.get(n) + 1)
                .forEach(m -> newGraph.putEdge(n, m));
            // @formatter:on
        }

        System.err.printf("After: %d edges\n", newGraph.edges().size());
        return newGraph;
    }

    static <KeyType, ValueType> String conflictsToDot(Collection<Transaction<KeyType, ValueType>> transactions,
            Collection<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>> edges,
            Collection<SIConstraint<KeyType, ValueType>> constraints) {
        StringBuilder builder = new StringBuilder();
        builder.append("digraph {\n");

        for (Transaction<KeyType, ValueType> txn : transactions) {
            builder.append(String.format("\"%s\";\n", txn));
        }

        for (Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>> e : edges) {
            EndpointPair<Transaction<KeyType, ValueType>> pair = e.getLeft();
            Collection<Edge<KeyType>> keys = e.getRight();
            StringBuilder label = new StringBuilder();

            for (Edge<KeyType> k : keys) {
                if (k.getType() != EdgeType.SO) {
                    label.append(String.format("%s %s\\n", k.getType(), k.getKey()));
                } else {
                    label.append(String.format("%s\\n", k.getType()));
                }
            }

            builder.append(
                    String.format("\"%s\" -> \"%s\" [label=\"%s\"];\n", pair.source(), pair.target(), label));
        }

        int colorStep = 0x1000000 / (constraints.size() + 1);
        int color = 0;
        for (SIConstraint<KeyType, ValueType> c : constraints) {
            color += colorStep;
            for (SIEdge<KeyType, ValueType> e : c.getEdges1()) {
                builder.append(String.format("\"%s\" -> \"%s\" [style=dotted,color=\"#%06x\"];\n", e.getFrom(), e.getTo(), color));
            }

            for (SIEdge<KeyType, ValueType> e : c.getEdges2()) {
                builder.append(String.format("\"%s\" -> \"%s\" [style=dashed,color=\"#%06x\"];\n", e.getFrom(), e.getTo(), color));
            }
        }

        builder.append("}\n");
        return builder.toString();
    }

    static <KeyType, ValueType> String conflictsToLegacy(Collection<Transaction<KeyType, ValueType>> transactions,
            Collection<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>> edges,
            Collection<SIConstraint<KeyType, ValueType>> constraints) {
        StringBuilder builder = new StringBuilder();

        edges.forEach(p -> builder.append(String.format("Edge: %s\n", p)));
        constraints.forEach(c -> builder.append(String.format("Constraint: %s\n", c)));
        builder.append(String.format("Related transactions:\n"));
        transactions.forEach(t -> {
            builder.append(String.format("sessionid: %d, id: %d\nops:\n", t.getSession().getId(), t.getId()));
            t.getEvents()
                    .forEach(e -> builder.append(String.format("%s %s = %s\n", e.getType(), e.getKey(), e.getValue())));
        });

        return builder.toString();
    }
}
