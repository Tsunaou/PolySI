package verifier;

import graph.KnownGraph;
import history.History;
import history.Transaction;
import util.Profiler;
import graph.Edge;
import graph.EdgeType;
import graph.MatrixGraph;

import java.util.*;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;

import lombok.Getter;
import lombok.Setter;

public class Pruning {
    @Getter
    @Setter
    private static boolean enablePruning = true;

    @Getter
    @Setter
    private static double stopThreshold = 0.01;

    static <KeyType, ValueType> boolean pruneConstraints(KnownGraph<KeyType, ValueType> knownGraph,
            Collection<SIConstraint<KeyType, ValueType>> constraints, History<KeyType, ValueType> history) {
        if (!enablePruning) {
            return false;
        }

        Profiler profiler = Profiler.getInstance();
        profiler.startTick("SI_PRUNE");

        int rounds = 1, solvedConstraints = 0, totalConstraints = constraints.size();
        boolean hasCycle = false;
        while (!hasCycle) {
            System.err.printf("Pruning round %d\n", rounds);
            Pair<Integer, Boolean> result = pruneConstraintsWithPostChecking(knownGraph, constraints, history);

            hasCycle = result.getRight();
            solvedConstraints += result.getLeft();

            if (result.getLeft() <= stopThreshold * totalConstraints
                    || totalConstraints - solvedConstraints <= stopThreshold * totalConstraints) {
                break;
            }
            rounds++;
        }

        profiler.endTick("SI_PRUNE");
        System.err.printf("Pruned %d rounds, solved %d constraints\n" + "After prune: graphA: %d, graphB: %d\n", rounds,
                solvedConstraints, knownGraph.getKnownGraphA().edges().size(),
                knownGraph.getKnownGraphB().edges().size());
        return hasCycle;
    }

    private static <KeyType, ValueType> Pair<Integer, Boolean> pruneConstraintsWithPostChecking(
            KnownGraph<KeyType, ValueType> knownGraph, Collection<SIConstraint<KeyType, ValueType>> constraints,
            History<KeyType, ValueType> history) {
        Profiler profiler = Profiler.getInstance();

        profiler.startTick("SI_PRUNE_POST_GRAPH_A_B");
        MatrixGraph<Transaction<KeyType, ValueType>> graphA = new MatrixGraph<>(knownGraph.getKnownGraphA().asGraph());
        MatrixGraph<Transaction<KeyType, ValueType>> graphB = new MatrixGraph<>(knownGraph.getKnownGraphB().asGraph(), graphA.getNodeMap());
        Map<Transaction<KeyType, ValueType>, Integer> orderInSession = Utils.getOrderInSession(history);
        profiler.endTick("SI_PRUNE_POST_GRAPH_A_B");

        profiler.startTick("SI_PRUNE_POST_GRAPH_C");
        MatrixGraph<Transaction<KeyType, ValueType>> graphC = graphA.composition(graphB);
        profiler.endTick("SI_PRUNE_POST_GRAPH_C");

        if (graphC.hasLoops()) {
            return Pair.of(0, true);
        }

        profiler.startTick("SI_PRUNE_POST_REACHABILITY");
        MatrixGraph<Transaction<KeyType, ValueType>> reachability = Utils.reduceEdges(graphA.union(graphC), orderInSession).reachability();
        System.err.printf("reachability matrix sparsity: %.2f\n",
                1 - reachability.nonZeroElements() / Math.pow(reachability.nodes().size(), 2));
        profiler.endTick("SI_PRUNE_POST_REACHABILITY");

        ArrayList<SIConstraint<KeyType, ValueType>> solvedConstraints = new ArrayList<SIConstraint<KeyType, ValueType>>();

        profiler.startTick("SI_PRUNE_POST_CHECK");
        for (SIConstraint<KeyType, ValueType> c : constraints) {
            Optional<SIEdge<KeyType, ValueType>> conflict = checkConflict(c.getEdges1(), reachability, knownGraph);
            if (conflict.isPresent()) {
                addToKnownGraph(knownGraph, c.getEdges2());
                solvedConstraints.add(c);
                // System.err.printf("%s -> %s because of conflict in %s\n",
                // c.writeTransaction2, c.writeTransaction1,
                // conflict.get());
                continue;
            }

            conflict = checkConflict(c.getEdges2(), reachability, knownGraph);
            if (conflict.isPresent()) {
                addToKnownGraph(knownGraph, c.getEdges1());
                // System.err.printf("%s -> %s because of conflict in %s\n",
                // c.writeTransaction1, c.writeTransaction2,
                // conflict.get());
                solvedConstraints.add(c);
            }
        }
        profiler.endTick("SI_PRUNE_POST_CHECK");

        System.err.printf("solved %d constraints\n", solvedConstraints.size());
        // constraints.removeAll(solvedConstraints);
        // java removeAll has performance bugs; do it manually
        solvedConstraints.forEach(constraints::remove);
        return Pair.of(solvedConstraints.size(), false);
    }

    private static <KeyType, ValueType> void addToKnownGraph(KnownGraph<KeyType, ValueType> knownGraph,
            Collection<SIEdge<KeyType, ValueType>> edges) {
        for (SIEdge<KeyType, ValueType> e : edges) {
            switch (e.getType()) {
            case WW:
                knownGraph.putEdge(e.getFrom(), e.getTo(), new Edge<KeyType>(EdgeType.WW, e.getKey()));
                break;
            case RW:
                knownGraph.putEdge(e.getFrom(), e.getTo(), new Edge<KeyType>(EdgeType.RW, e.getKey()));
                break;
            default:
                throw new Error("only WW and RW edges should appear in constraints");
            }
        }
    }

    private static <KeyType, ValueType> Optional<SIEdge<KeyType, ValueType>> checkConflict(
            Collection<SIEdge<KeyType, ValueType>> edges, MatrixGraph<Transaction<KeyType, ValueType>> reachability,
            KnownGraph<KeyType, ValueType> knownGraph) {
        for (SIEdge<KeyType, ValueType> e : edges) {
            switch (e.getType()) {
            case WW:
                if (reachability.hasEdgeConnecting(e.getTo(), e.getFrom())) {
                    return Optional.of(e);
                    // System.err.printf("conflict edge: %s\n", e);
                }
                break;
            case RW:
                for (Transaction<KeyType, ValueType> n : knownGraph.getKnownGraphA().predecessors(e.getFrom())) {
                    if (reachability.hasEdgeConnecting(e.getTo(), n)) {
                        return Optional.of(e);
                        // System.err.printf("conflict edge: %s\n", e);
                    }
                }
                break;
            default:
                throw new Error("only WW and RW edges should appear in constraints");
            }
        }

        return Optional.empty();
    }
}
