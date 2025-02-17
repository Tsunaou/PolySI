package history.loaders;

import static history.Event.EventType.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

import history.*;
import history.History.*;
import lombok.SneakyThrows;

public class CobraHistoryLoader implements HistoryParser<Long, CobraHistoryLoader.CobraValue> {
	private final File logDir;

	public static long INIT_WRITE_ID = 0xbebeebeeL;
	public static long INIT_TXN_ID = 0xbebeebeeL;
	public static long NULL_TXN_ID = 0xdeadbeefL;
	public static long GC_WID_TRUE = 0x23332333L;
	public static long GC_WID_FALSE = 0x66666666L;

	public CobraHistoryLoader(Path path) {
		logDir = path.toFile();

		if (!logDir.isDirectory()) {
			throw new Error("path is not a directory");
		}
	}

	@Override
	public History<Long, CobraValue> loadHistory() {
        ArrayList<File> files = findLogWithSuffix(".log");
		return loadLogs(files);
	}

	private ArrayList<File> findLogWithSuffix(String suffix) {
		ArrayList<File> logs = new ArrayList<File>();
		for (File f : logDir.listFiles()) {
			if (f.isFile() && f.getName().endsWith(suffix)) {
				logs.add(f);
			}
		}
		return logs;
	}

	@SneakyThrows
	private History<Long, CobraValue> loadLogs(ArrayList<File> opfiles) {
        History<Long, CobraValue> history = new History<Long, CobraValue>();
        HashMap<Long, CobraValue> initWrites = new HashMap<Long, CobraValue>();
        int sessionId = 0;

		for (File f : opfiles) {
			try (DataInputStream in = new DataInputStream(new FileInputStream(f))) {
                Session<Long, CobraValue> session = history.addSession(sessionId++);
				extractLog(in, history, initWrites, session);
			}
		}

        Transaction<Long, CobraValue> initTxn = history.addTransaction(history.addSession(INIT_TXN_ID), INIT_TXN_ID);
		for (Map.Entry<Long, CobraValue> p : initWrites.entrySet()) {
			history.addEvent(initTxn, WRITE, p.getKey(), p.getValue());
		}
		initTxn.setStatus(Transaction.TransactionStatus.COMMIT);

		for (Transaction<Long, CobraValue> txn : history.getTransactions()) {
			if (txn.getStatus() != Transaction.TransactionStatus.COMMIT) {
				throw new InvalidHistoryError();
			}
		}

		return history;
	}

	/*
	 * Extract the graph from a stream (a byte stream from cloud or a file stream
	 * from log) with client timing edges.
	 *
	 * (startTx, txnId [, timestamp]) : 9B [+8B] <br>
	 *
	 * (commitTx, txnId [, timestamp]) : 9B [+8B] <br>
	 *
	 * (write, writeId, key_hash, val): 25B <br>
	 *
	 * (read, write_TxnId, writeId, key_hash, value) : 33B <br>
	 */
	@SneakyThrows
	public void extractLog(DataInputStream in, History<Long, CobraValue> history, Map<Long, CobraValue> initWrites,
			Session<Long, CobraValue> session) {
		Transaction<Long, CobraValue> current = null;
		while (true) {
			// break if end (for file)
			char op;
			try {
				op = (char) in.readByte();
			} catch (EOFException e) {
				break;
			}

			switch (op) {
			case 'S': {
				// TxnStart
				assert current == null;
                long id = in.readLong();

				// NOTE: because of inconsistency of the logs, the node might be created already
				// There are two possibilities:
				// 1. in graph and ongoing: continue (previous txn reads this txn)
				// 2. never seen: new TxnNode & continue
				if ((current = history.getTransaction(id)) == null) {
					current = history.addTransaction(session, id);
				} else if (current.getStatus() != Transaction.TransactionStatus.ONGOING || current.getEvents().size() != 0) {
					throw new InvalidHistoryError();
				}
				break;
			}
			case 'C': {
				// TxnCommit
                long id = in.readLong();
				assert current != null && current.getId() == id;
				current.setStatus(Transaction.TransactionStatus.COMMIT);
				break;
			}
			case 'W': {
				// (write, writeId, key, val): ?B <br>
				assert current != null;
                long writeId = in.readLong();
                long key = in.readLong();
                long value = in.readLong();

				// use writeId as value because cobra guarantees its uniqueness
				history.addEvent(current, WRITE, key, new CobraValue(writeId, current.getId(), value));
				break;
			}
			case 'R': {
				// (read, write_TxnId, writeId, key, value) : ?B <br>
				assert current != null;
                long writeTxnId = in.readLong();
                long writeId = in.readLong();
                long key = in.readLong();
                long value = in.readLong();

				// NOTE: if the prev_txnid == INIT_TXNID, then we update it to keyhash as wid
				// FIXME: separate NULL and INIT?
				if (writeTxnId == INIT_TXN_ID || writeTxnId == NULL_TXN_ID) {
					if (writeId == INIT_WRITE_ID || writeId == NULL_TXN_ID) {
						// NOTE: val_hash is 0 for NULL; but an arbitrary value for INIT
						writeId = key;
						writeTxnId = INIT_TXN_ID;
						// if there is no such write op in init txn, add one
						initWrites.computeIfAbsent(key, k -> new CobraValue(key, INIT_TXN_ID, value));
					} else if ((writeId != GC_WID_FALSE && writeId != GC_WID_TRUE) || writeTxnId != INIT_TXN_ID) {
						// otherwise, it should be the GC read op
						throw new InvalidHistoryError();
					}
				}

				history.addEvent(current, READ, key, new CobraValue(writeId, writeTxnId, value));
				break;
			}
			default:
				throw new InvalidHistoryError();
			}
		}
	}

	@Override
	@SneakyThrows
	public void dumpHistory(History<Long, CobraValue> history) {
		if (!logDir.isDirectory()) {
			throw new Error(String.format("%s is not a directory", logDir));
		}
		Arrays.stream(logDir.listFiles()).forEach(f -> f.delete());

		for (Session<Long, CobraValue> session : history.getSessions()) {
			try (DataOutputStream out = new DataOutputStream(new FileOutputStream(
					logDir.toPath().resolve(String.format("T%d.log", session.getId())).toFile()))) {
				for (Transaction<Long, CobraValue> transaction : session.getTransactions()) {
					out.writeByte('S');
					out.writeLong(transaction.getId());

					for (Event<Long, CobraValue> event : transaction.getEvents()) {
						switch (event.getType()) {
						case WRITE: {
							out.writeByte('W');
                            CobraValue value = event.getValue();
							out.writeLong(value.getWriteId());
							out.writeLong(event.getKey());
							out.writeLong(value.getValue());
							break;
						}
						case READ: {
							out.writeByte('R');
                            CobraValue value = event.getValue();
							out.writeLong(value.getTransactionId());
							out.writeLong(value.getWriteId());
							out.writeLong(event.getKey());
							out.writeLong(value.getValue());
							break;
						}
						}
					}

					out.writeByte('C');
					out.writeLong(transaction.getId());
				}
			}
		}
	}

	@Override
	public <T, U> History<Long, CobraValue> convertFrom(History<T, U> history) {
        Collection<Event<T, U>> events = history.getEvents();
        HashMap<T, Long> keys = Utils.getIdMap(events.stream().map(ev -> ev.getKey()), 1);
        HashMap<Pair<T, U>, Long> writes = Utils.getIdMap(
				events.stream().filter(ev -> ev.getType() == WRITE).map(ev -> Pair.of(ev.getKey(), ev.getValue())),
				0x10000000);
        Map<Pair<T, U>, Long> writeTxns = events.stream().filter(ev -> ev.getType() == WRITE)
				.map(ev -> Pair.of(Pair.of(ev.getKey(), ev.getValue()), ev.getTransaction().getId()))
				.collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        HashSet<Pair<Transaction<T, U>, T>> txnReads = new HashSet<Pair<Transaction<T, U>, T>>();
        HashMap<Pair<Transaction<T, U>, T>, Integer> txnWrites = new HashMap<Pair<Transaction<T, U>, T>, Integer>();
        HashMap<Pair<Transaction<T, U>, T>, Integer> writeCount = new HashMap<Pair<Transaction<T, U>, T>, Integer>();
        BiFunction<Pair<Transaction<T, U>, T>, Integer, Integer> incCount = ((BiFunction<Pair<Transaction<T, U>, T>, Integer, Integer>) (k, c) -> {
			if (c == null) {
				return 1;
			}
			return c + 1;
		});
		events.stream().filter(ev -> ev.getType() == WRITE)
				.forEach(ev -> writeCount.compute(Pair.of(ev.getTransaction(), ev.getKey()), incCount));

		return Utils.convertHistory(history, ev -> {
            Pair<T, U> kv = Pair.of(ev.getKey(), ev.getValue());
            Long id = writes.get(kv);
            long txnId = ev.getType() == WRITE ? ev.getTransaction().getId() : writeTxns.get(kv);
			return Pair.of(keys.get(ev.getKey()), new CobraValue(id, txnId, id));
		}, ev -> {
            Pair<Transaction<T, U>, T> txnAndKey = Pair.of(ev.getTransaction(), ev.getKey());
			switch (ev.getType()) {
			case READ:
				// only allow one read per txn and must read from others
				// leave only the first read if it is before writes
				if (txnWrites.containsKey(txnAndKey) || txnReads.contains(txnAndKey)) {
					return false;
				}
				txnReads.add(txnAndKey);
				break;
			case WRITE:
				// only allow one write per txn
				// leave only the last write
                Integer count = txnWrites.compute(txnAndKey, incCount);
				if (count < writeCount.get(txnAndKey)) {
					return false;
				}
				break;
			}
			return true;
		});
	}

	@Data
	public static class CobraValue {
		private final long writeId;
		private final long transactionId;
		private final long value;
	}
}
