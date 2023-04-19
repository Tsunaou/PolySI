package verifier;

import com.google.common.graph.EndpointPair;
import com.google.common.graph.MutableValueGraph;
import graph.Edge;
import graph.EdgeType;
import graph.KnownGraph;
import history.Event;
import history.History;
import history.HistoryLoader;
import history.Transaction;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;

import util.Profiler;
import util.TriConsumer;

@SuppressWarnings("UnstableApiUsage")
public class SIVerifier<KeyType, ValueType> {
    private final History<KeyType, ValueType> history;

    @Getter
    @Setter
    private static boolean coalesceConstraints = true;

    @Getter
    @Setter
    private static boolean dotOutput = false;

    public SIVerifier(HistoryLoader<KeyType, ValueType> loader) {
        history = loader.loadHistory();
        System.err.printf("Sessions count: %d\nTransactions count: %d\nEvents count: %d\n",
                history.getSessions().size(), history.getTransactions().size(), history.getEvents().size());
    }

    public boolean audit() {
        Profiler profiler = Profiler.getInstance();

        profiler.startTick("ONESHOT_CONS");
        profiler.startTick("SI_VERIFY_INT");
        boolean satisfy_int = Utils.verifyInternalConsistency(history);
        profiler.endTick("SI_VERIFY_INT");
        if (!satisfy_int) {
            return false;
        }

        profiler.startTick("SI_GEN_PREC_GRAPH");
        KnownGraph<KeyType, ValueType> graph = new KnownGraph<>(history);
        profiler.endTick("SI_GEN_PREC_GRAPH");
        System.err.printf("Known edges: %d\n", graph.getKnownGraphA().edges().size());

        profiler.startTick("SI_GEN_CONSTRAINTS");
        Collection<SIConstraint<KeyType, ValueType>> constraints = generateConstraints(history, graph);
        profiler.endTick("SI_GEN_CONSTRAINTS");
        System.err.printf("Constraints count: %d\nTotal edges in constraints: %d\n", constraints.size(),
                constraints.stream().map(c -> c.getEdges1().size() + c.getEdges2().size()).reduce(0, Integer::sum));
        profiler.endTick("ONESHOT_CONS");

        boolean hasLoop = Pruning.pruneConstraints(graph, constraints, history);
        if (hasLoop) {
            System.err.printf("Cycle found in pruning\n");
        }
        System.err.printf("After Prune:\n" + "Constraints count: %d\nTotal edges in constraints: %d\n",
                constraints.size(),
                constraints.stream().map(c -> c.getEdges1().size() + c.getEdges2().size()).reduce(0, Integer::sum));

        profiler.startTick("ONESHOT_SOLVE");
        SISolver<KeyType, ValueType> solver = new SISolver<>(history, graph, constraints);

        boolean accepted = solver.solve();
        profiler.endTick("ONESHOT_SOLVE");

        if (!accepted) {
            Pair<Collection<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>>, Collection<SIConstraint<KeyType, ValueType>>> conflicts = solver.getConflicts();
            HashSet<Transaction<KeyType, ValueType>> txns = new HashSet<Transaction<KeyType, ValueType>>();

            conflicts.getLeft().forEach(e -> {
                txns.add(e.getLeft().source());
                txns.add(e.getLeft().target());
            });
            conflicts.getRight().forEach(c -> {
                Consumer<Collection<SIEdge<KeyType, ValueType>>> addEdges = ((Consumer<Collection<SIEdge<KeyType, ValueType>>>) s -> s.forEach(e -> {
                    txns.add(e.getFrom());
                    txns.add(e.getTo());
                }));
                addEdges.accept(c.getEdges1());
                addEdges.accept(c.getEdges2());
            });

            if (dotOutput) {
                System.out.print(Utils.conflictsToDot(txns, conflicts.getLeft(), conflicts.getRight()));
            } else {
                System.out.print(Utils.conflictsToLegacy(txns, conflicts.getLeft(), conflicts.getRight()));
            }
        }

        return accepted;
    }

    /*
     * Generate constraints from a precedence graph. Use coalescing to reduce the
     * number of constraints produced.
     *
     * @param graph the graph to use
     *
     * @return the set of constraints generated
     *
     * For each pair of transactions A, C, generate the following constraint:
     *
     * 1. A precedes C, add A ->(ww) C. Let K be a key written by both A and C, for
     * each transaction B such that A ->(wr, K) B, add B ->(rw) C.
     *
     * 2. C precedes A, add C ->(ww) A. For each transaction B such that C ->(wr, K)
     * A, add B ->(rw) A.
     */
    private static <KeyType, ValueType> Collection<SIConstraint<KeyType, ValueType>> generateConstraintsCoalesce(
            History<KeyType, ValueType> history, KnownGraph<KeyType, ValueType> graph) {
        MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Edge<KeyType>>> readFrom = graph.getReadFrom();
        HashMap<KeyType, Set<Transaction<KeyType, ValueType>>> writes = new HashMap<KeyType, Set<Transaction<KeyType, ValueType>>>();

        history.getEvents().stream().filter(e -> e.getType() == Event.EventType.WRITE).forEach(ev -> {
            writes.computeIfAbsent(ev.getKey(), k -> new HashSet<>()).add(ev.getTransaction());
        });

        Consumer<TriConsumer<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>, KeyType>> forEachWriteSameKey = ((Consumer<TriConsumer<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>, KeyType>>) f -> {
            for (Map.Entry<KeyType, Set<Transaction<KeyType, ValueType>>> p : writes.entrySet()) {
                KeyType key = p.getKey();
                ArrayList<Transaction<KeyType, ValueType>> list = new ArrayList<>(p.getValue());
                for (int i = 0; i < list.size(); i++) {
                    for (int j = i + 1; j < list.size(); j++) {
                        f.accept(list.get(i), list.get(j), key);
                    }
                }
            }
        });

        HashMap<Pair<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>>, Collection<SIEdge<KeyType, ValueType>>> constraintEdges = new HashMap<Pair<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>>, Collection<SIEdge<KeyType, ValueType>>>();
        forEachWriteSameKey.accept((a, c, key) -> {
            BiConsumer<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>> addEdge = ((BiConsumer<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>>) (m, n) -> {
                constraintEdges.computeIfAbsent(Pair.of(m, n), p -> new ArrayList<>())
                        .add(new SIEdge<>(m, n, EdgeType.WW, key));
            });
            addEdge.accept(a, c);
            addEdge.accept(c, a);
        });

        for (Transaction<KeyType, ValueType> a : history.getTransactions()) {
            for (Transaction<KeyType, ValueType> b : readFrom.successors(a)) {
                for (Edge<KeyType> edge : readFrom.edgeValue(a, b).get()) {
                    for (Transaction<KeyType, ValueType> c : writes.get(edge.getKey())) {
                        if (a == c || b == c) {
                            continue;
                        }

                        constraintEdges.get(Pair.of(a, c)).add(new SIEdge<>(b, c, EdgeType.RW, edge.getKey()));
                    }
                }
            }
        }

        HashSet<SIConstraint<KeyType, ValueType>> constraints = new HashSet<SIConstraint<KeyType, ValueType>>();
        HashSet<Pair<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>>> addedPairs = new HashSet<Pair<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>>>();
        AtomicInteger constraintId = new AtomicInteger();
        forEachWriteSameKey.accept((a, c, key) -> {
            if (addedPairs.contains(Pair.of(a, c)) || addedPairs.contains(Pair.of(c, a))) {
                return;
            }
            addedPairs.add(Pair.of(a, c));
            constraints.add(new SIConstraint<>(constraintEdges.get(Pair.of(a, c)), constraintEdges.get(Pair.of(c, a)),
                    a, c, constraintId.getAndIncrement()));
        });

        return constraints;
    }

    private static <KeyType, ValueType> Collection<SIConstraint<KeyType, ValueType>> generateConstraintsNoCoalesce(
            History<KeyType, ValueType> history, KnownGraph<KeyType, ValueType> graph) {
        MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Edge<KeyType>>> readFrom = graph.getReadFrom();
        HashMap<KeyType, Set<Transaction<KeyType, ValueType>>> writes = new HashMap<KeyType, Set<Transaction<KeyType, ValueType>>>();

        history.getEvents().stream().filter(e -> e.getType() == Event.EventType.WRITE).forEach(ev -> {
            writes.computeIfAbsent(ev.getKey(), k -> new HashSet<>()).add(ev.getTransaction());
        });

        HashSet<SIConstraint<KeyType, ValueType>> constraints = new HashSet<SIConstraint<KeyType, ValueType>>();
        int constraintId = 0;
        for (Transaction<KeyType, ValueType> a : history.getTransactions()) {
            for (Transaction<KeyType, ValueType> b : readFrom.successors(a)) {
                for (Edge<KeyType> edge : readFrom.edgeValue(a, b).get()) {
                    for (Transaction<KeyType, ValueType> c : writes.get(edge.getKey())) {
                        if (a == c || b == c) {
                            continue;
                        }

                        constraints.add(new SIConstraint<>(
                                List.of(new SIEdge<>(a, c, EdgeType.WW, edge.getKey()),
                                        new SIEdge<>(b, c, EdgeType.RW, edge.getKey())),
                                List.of(new SIEdge<>(c, a, EdgeType.WW, edge.getKey())), a, c, constraintId++));
                    }
                }
            }
        }
        for (Map.Entry<KeyType, Set<Transaction<KeyType, ValueType>>> write : writes.entrySet()) {
            ArrayList<Transaction<KeyType, ValueType>> list = new ArrayList<>(write.getValue());
            for (int i = 0; i < list.size(); i++) {
                for (int j = i + 1; j < list.size(); j++) {
                    Transaction<KeyType, ValueType> a = list.get(i);
                    Transaction<KeyType, ValueType> c = list.get(j);
                    constraints.add(new SIConstraint<>(List.of(new SIEdge<>(a, c, EdgeType.WW, write.getKey())),
                            List.of(new SIEdge<>(c, a, EdgeType.WW, write.getKey())), a, c, constraintId++));
                }
            }
        }

        return constraints;
    }

    private static <KeyType, ValueType> Collection<SIConstraint<KeyType, ValueType>> generateConstraints(
            History<KeyType, ValueType> history, KnownGraph<KeyType, ValueType> graph) {
        if (coalesceConstraints) {
            return generateConstraintsCoalesce(history, graph);
        }
        return generateConstraintsNoCoalesce(history, graph);
    }
}
