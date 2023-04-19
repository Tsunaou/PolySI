package history.loaders;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import history.*;
import history.Event.EventType;

import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;

public class TextHistoryLoader implements HistoryParser<Long, Long> {
    private final File textFile;

    public TextHistoryLoader(Path filePath) {
        textFile = filePath.toFile();
    }

    @Override
    public <T, U> History<Long, Long> convertFrom(History<T, U> history) {
        Collection<Event<T, U>> events = history.getEvents();
        HashMap<T, Long> keys = Utils.getIdMap(events.stream().map(Event::getKey), 1);
        HashMap<U, Long> values = Utils.getIdMap(events.stream().map(Event::getValue), 1);

        return Utils.convertHistory(history,
                ev -> Pair.of(keys.get(ev.getKey()), values.get(ev.getValue())),
                ev -> true);
    }

    @Override
    @SneakyThrows
    public History<Long, Long> loadHistory() {
        @Cleanup
        BufferedReader in = new BufferedReader(new FileReader(textFile));
        History<Long, Long> history = new History<Long, Long>();
        Pattern regex = Pattern
                .compile("(r|w)\\((\\d++),(\\d++),(\\d++),(\\d++)\\)");

        Session<Long, Long> initSession = history.addSession(-1);
        Transaction<Long, Long> initTxn = history.addTransaction(initSession, -1);
        HashSet<Long> keySet = new HashSet<Long>();

        in.lines().forEachOrdered((line) -> {
            Matcher match = regex.matcher(line);
            if (!match.matches()) {
                throw new Error("Invalid format");
            }

            String op = match.group(1);
            long key = Long.parseLong(match.group(2));
            long value = Long.parseLong(match.group(3));
            long session = Long.parseLong(match.group(4));
            long txn = Long.parseLong(match.group(5));

            if (history.getSession(session) == null) {
                history.addSession(session);
            }

            if (history.getTransaction(txn) == null) {
                history.addTransaction(history.getSession(session), txn);
            }

            if (!keySet.contains(key)) {
                keySet.add(key);
                history.addEvent(initTxn, EventType.WRITE, key, 0L);
            }

            history.addEvent(history.getTransaction(txn),
                    op.equals("r") ? EventType.READ : EventType.WRITE, key,
                    value);
        });

        return history;
    }

    @Override
    @SneakyThrows
    public void dumpHistory(History<Long, Long> history) {
        @Cleanup
        BufferedWriter out = new BufferedWriter(new FileWriter(textFile));

        for (Transaction<Long, Long> txn : history.getTransactions()) {
            for (Event<Long, Long> ev : txn.getEvents()) {
                out.append(String.format("%s(%d,%d,%d,%d)\n",
                        ev.getType().equals(EventType.READ) ? "r" : "w",
                        ev.getKey(), ev.getValue(),
                        txn.getSession().getId(),
                        txn.getId()));
            }
        }
    }
}
