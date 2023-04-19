package history.loaders;

import static history.Event.EventType.READ;
import static history.Event.EventType.WRITE;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.io.LittleEndianDataOutputStream;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import history.*;
import history.History.*;

@SuppressWarnings("UnstableApiUsage")
public class DBCopHistoryLoader implements HistoryParser<Long, Long> {
	private final File logFile;

	private static final long INIT_SESSION_ID = 0;
	private static final long INIT_TXN_ID = 0;

	public DBCopHistoryLoader(Path logPath) {
		logFile = logPath.toFile();
	}

	@Override
	@SneakyThrows
	public History<Long, Long> loadHistory() {
		try (LittleEndianDataInputStream in = new LittleEndianDataInputStream(new BufferedInputStream(new FileInputStream(logFile)))) {
			return (new InternalLoader(in)).load();
		}
	}

	@Override
	@SneakyThrows
	public void dumpHistory(History<Long, Long> history) {
		try (LittleEndianDataOutputStream out = new LittleEndianDataOutputStream(new BufferedOutputStream(new FileOutputStream(logFile)))) {
			(new InternalDumper(history, out)).dump();
		}
	}

	@RequiredArgsConstructor
	private static class InternalLoader {
		private final History<Long, Long> history = new History<>();
		private final Set<Long> keys = new HashSet<>();
		private long sessionId = 1;
		private long transactionId = 1;
		private final LittleEndianDataInputStream in;

		@SneakyThrows
		History<Long, Long> load() {
			parseHistory();

            Transaction<Long, Long> init = history.addTransaction(history.addSession(0), 0);
			for (Long k : keys) {
				history.addEvent(init, WRITE, k, 0L);
			}

			return history;
		}

		@SneakyThrows
		private void parseHistory() {
            long id = in.readLong();
            long nodeNum = in.readLong();
            long variableNum = in.readLong();
            long transactionNum = in.readLong();
            long eventNum = in.readLong();
            String info = parseString();
            String start = parseString();
            String end = parseString();

            long length = in.readLong();
			for (long i = 0; i < length; i++) {
				parseSession();
			}
		}

		@SneakyThrows
		private String parseString() {
            // long size = in.readLong();
			// assert size <= Integer.MAX_VALUE;
			// return new String(in.readNBytes((int) size), StandardCharsets.UTF_8);

			long size = in.readLong();
			assert size <= Integer.MAX_VALUE;
			byte[] buffer = new byte[(int) size];
			int bytesRead = 0;
			while (bytesRead < size) {
				int count = in.read(buffer, bytesRead, (int) size - bytesRead);
				if (count == -1) {
					break;
				}
				bytesRead += count;
			}
			return new String(buffer, StandardCharsets.UTF_8);
		}

		@SneakyThrows
		void parseSession() {
            long length = in.readLong();
            Session<Long, Long> session = history.addSession(sessionId++);
			for (long i = 0; i < length; i++) {
				parseTransaction(session);
			}
		}

		@SneakyThrows
		void parseTransaction(Session<Long, Long> session) {
            long length = in.readLong();
            ArrayList<Triple<Event.EventType, Long, Long>> events = new ArrayList<Triple<Event.EventType, Long, Long>>();
			for (long i = 0; i < length; i++) {
                boolean write = in.readBoolean();
                long key = in.readLong();
                long value = in.readLong();
                boolean success = in.readBoolean();

				if (success) {
					keys.add(key);
					events.add(Triple.of(write ? WRITE : READ, key, value));
				}
			}

            boolean success = in.readBoolean();
			if (success) {
                Transaction<Long, Long> txn = history.addTransaction(session, transactionId++);
				events.forEach(t -> history.addEvent(txn, t.getLeft(), t.getMiddle(), t.getRight()));
			}
		}
	}

	@RequiredArgsConstructor
	private static class InternalDumper {
		private final History<Long, Long> history;
		private final LittleEndianDataOutputStream out;

		@SneakyThrows
		void dump() {
			out.writeLong(0); // id
			out.writeLong(history.getSessions().size()); // nodeNum
			out.writeLong(history.getEvents().stream().map(ev -> ev.getKey()).collect(Collectors.toSet()).size()); // variableNum
			out.writeLong(history.getTransactions().size()); // transactionNum
			out.writeLong(history.getEvents().size()); // eventNum
			dumpString("generated by SIVerifier"); // info
            String d = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX").format(new Date());
			dumpString(d); // start
			dumpString(d); // end

			out.writeLong(history.getSessions().size());
			for (Session<Long, Long> s : history.getSessions()) {
				dumpSession(s);
			}
		}

		@SneakyThrows
		void dumpSession(Session<Long, Long> session) {
			out.writeLong(session.getTransactions().size());
			for (Transaction<Long, Long> txn : session.getTransactions()) {
				dumpTransaction(txn);
			}
		}

		@SneakyThrows
		void dumpTransaction(Transaction<Long, Long> transaction) {
			out.writeLong(transaction.getEvents().size());
			for (Event<Long, Long> ev : transaction.getEvents()) {
				out.writeBoolean(ev.getType() == WRITE);
				out.writeLong(ev.getKey());
				out.writeLong(ev.getValue());
				out.writeBoolean(true); // success
			}

			out.writeBoolean(true); // success
		}

		@SneakyThrows
		void dumpString(String str) {
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
			out.writeLong(bytes.length);
			out.write(bytes);
		}
	}

	@Override
	public <T, U> History<Long, Long> convertFrom(History<T, U> history) {
        Collection<Event<T, U>> events = history.getEvents();
        HashMap<T, Long> keys = Utils.getIdMap(events.stream().map(Event::getKey), 1);
        HashMap<U, Long> values = Utils.getIdMap(events.stream().map(Event::getValue), 1);

		return Utils.convertHistory(history, ev -> Pair.of(keys.get(ev.getKey()), values.get(ev.getValue())),
				ev -> true);
	}
}
