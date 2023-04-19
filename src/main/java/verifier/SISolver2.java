package verifier;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Streams;

import com.google.common.graph.EndpointPair;
import com.google.common.graph.Graph;
import graph.Edge;
import graph.EdgeType;
import graph.KnownGraph;
import graph.MatrixGraph;
import history.History;
import history.Transaction;
import monosat.Lit;
import monosat.Logic;
import monosat.Solver;
import org.apache.commons.lang3.tuple.Pair;

import util.Profiler;

class SISolver2<KeyType, ValueType> {
    private Lit[][] graphABEdges;
    private Lit[][] graphACEdges;

    private Solver solver = new Solver();

    private HashSet<Lit> knownLits = new HashSet<>();
    private HashSet<Lit> constraintLits = new HashSet<>();

    SISolver2(History<KeyType, ValueType> history,
            KnownGraph<KeyType, ValueType> precedenceGraph,
            Collection<SIConstraint<KeyType, ValueType>> constraints) {
        Profiler profiler = Profiler.getInstance();
        profiler.startTick("SI_SOLVER2_CONSTRUCT");

        HashMap<Transaction<KeyType, ValueType>, Integer> nodeMap = new HashMap<Transaction<KeyType, ValueType>, Integer>();
        {
            int i = 0;
            for (Transaction<KeyType, ValueType> txn : history.getTransactions()) {
                nodeMap.put(txn, i++);
            }
        }

        Function<Supplier<Lit>, Lit[][]> createLitMatrix = ((Function<Supplier<Lit>, Lit[][]>) newLit -> {
            int n = history.getTransactions().size();
            Lit[][] lits = new Lit[n][n];
            for (int j = 0; j < n; j++) {
                for (int k = 0; k < n; k++) {
                    lits[j][k] = newLit.get();
                }
            }

            return lits;
        });
        graphABEdges = createLitMatrix.apply(() -> {
            Lit lit = new Lit(solver);
            solver.setDecisionLiteral(lit, false);
            return lit;
        });
        graphACEdges = createLitMatrix.apply(() -> Lit.False);

        for (EndpointPair<Transaction<KeyType, ValueType>> e : precedenceGraph.getKnownGraphA().edges()) {
            knownLits.add(graphABEdges[nodeMap.get(e.source())][nodeMap
                    .get(e.target())]);
        }
        for (EndpointPair<Transaction<KeyType, ValueType>> e : precedenceGraph.getKnownGraphB().edges()) {
            knownLits.add(graphABEdges[nodeMap.get(e.source())][nodeMap
                    .get(e.target())]);
        }

        BiFunction<Lit, Collection<SIEdge<KeyType, ValueType>>, Lit> impliesCNF = ((BiFunction<Lit, Collection<SIEdge<KeyType, ValueType>>, Lit>) (
                lit, edges) -> edges.stream()
                        .filter(e -> e.getType().equals(EdgeType.RW))
                        .map(e -> graphABEdges[nodeMap.get(e.getFrom())][nodeMap
                                .get(e.getTo())])
                        .map(l -> Logic.or(Logic.not(lit), l))
                        .reduce(Lit.True, Logic::and));
        for (SIConstraint<KeyType, ValueType> c : constraints) {
            Integer i = nodeMap.get(c.getWriteTransaction1());
            Integer j = nodeMap.get(c.getWriteTransaction2());
            // var either = impliesCNF.apply(graphABEdges[i][j], c.getEdges1());
            // var or = impliesCNF.apply(graphABEdges[j][i], c.getEdges2());
            Lit either = Logic.implies(graphABEdges[i][j],
                    c.getEdges1().stream()
                            .filter(e -> e.getType().equals(EdgeType.RW))
                            .map(e -> graphABEdges[nodeMap
                                    .get(e.getFrom())][nodeMap.get(e.getTo())])
                            .reduce(Lit.True, Logic::and));
            Lit or = Logic.implies(graphABEdges[j][i],
                    c.getEdges2().stream()
                            .filter(e -> e.getType().equals(EdgeType.RW))
                            .map(e -> graphABEdges[nodeMap
                                    .get(e.getFrom())][nodeMap.get(e.getTo())])
                            .reduce(Lit.True, Logic::and));

            constraintLits.add(either);
            constraintLits.add(or);
            constraintLits
                    .add(Logic.xor(graphABEdges[i][j], graphABEdges[j][i]));
            solver.setDecisionLiteral(graphABEdges[i][j], true);
            solver.setDecisionLiteral(graphABEdges[j][i], true);
        }

        MatrixGraph<Transaction<KeyType, ValueType>> matA = new MatrixGraph<>(
                precedenceGraph.getKnownGraphA().asGraph());
        Map<Transaction<KeyType, ValueType>, Integer> orderInSession = Utils.getOrderInSession(history);
        MatrixGraph<Transaction<KeyType, ValueType>> minimalAUnionC = Utils.reduceEdges(
                matA.union(matA.composition(new MatrixGraph<>(
                        precedenceGraph.getKnownGraphB().asGraph()))),
                orderInSession);
        MatrixGraph<Transaction<KeyType, ValueType>> reachability = minimalAUnionC.reachability();
        BiFunction<Graph<Transaction<KeyType, ValueType>>, EdgeType, List<Pair<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>>>> collectEdges = ((BiFunction<Graph<Transaction<KeyType, ValueType>>, EdgeType, List<Pair<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>>>>) (
                known, type) -> Stream
                        .concat(known.edges().stream()
                                .map(e -> Pair.of(e.source(), e.target())),
                                constraints.stream()
                                        .flatMap(c -> Stream.concat(
                                                c.getEdges1().stream(),
                                                c.getEdges2().stream()))
                                        .filter(e -> e.getType().equals(type))
                                        .map(e -> Pair.of(e.getFrom(),
                                                e.getTo())))
                        .collect(Collectors.toList()));
        List<Pair<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>>> edgesInA = collectEdges
                .apply(precedenceGraph.getKnownGraphA().asGraph(), EdgeType.WW);
        List<Pair<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>>> edgesInB = collectEdges
                .apply(precedenceGraph.getKnownGraphB().asGraph(), EdgeType.RW);
        for (Pair<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>> e1 : edgesInA) {
            Integer vi = nodeMap.get(e1.getLeft());
            Integer vj = nodeMap.get(e1.getRight());
            graphACEdges[vi][vj] = Logic.or(graphACEdges[vi][vj],
                    graphABEdges[vi][vj]);
            solver.setDecisionLiteral(graphACEdges[vi][vj], false);
        }
        for (Pair<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>> e1 : edgesInA) {
            for (Pair<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>> e2 : edgesInB) {
                if (!e1.getRight().equals(e2.getLeft())) {
                    continue;
                }

                Integer vi = nodeMap.get(e1.getLeft());
                Integer vj = nodeMap.get(e1.getRight());
                Integer vk = nodeMap.get(e2.getRight());
                graphACEdges[vi][vk] = Logic.or(graphACEdges[vi][vk],
                        Logic.and(graphABEdges[vi][vj], graphABEdges[vj][vk]));
                solver.setDecisionLiteral(graphACEdges[vi][vk], false);
            }
        }

        monosat.Graph monoGraph = new monosat.Graph(solver);
        int[] nodes = new int[graphACEdges.length];
        for (int i = 0; i < graphACEdges.length; i++) {
            nodes[i] = monoGraph.addNode();
        }
        for (int i = 0; i < graphACEdges.length; i++) {
            for (int j = 0; j < graphACEdges[i].length; j++) {
                Lit lit = monoGraph.addEdge(nodes[i], nodes[j]);
                solver.assertEqual(lit, graphACEdges[i][j]);
            }
        }

        solver.assertTrue(monoGraph.acyclic());

        profiler.endTick("SI_SOLVER2_CONSTRUCT");
    }

    public boolean solve() {
        List<Lit> lits = Stream.concat(knownLits.stream(), constraintLits.stream())
                .collect(Collectors.toList());

        return solver.solve(lits);
    }

    Pair<Collection<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>>, Collection<SIConstraint<KeyType, ValueType>>> getConflicts() {
        return Pair.of(Collections.emptyList(), Collections.emptyList());
    }
}
