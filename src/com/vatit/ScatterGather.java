package com.vatit;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * http://www.java-allandsundry.com/2015/07/scatter-gather-using-java-8.html
 * TODO how to pass payload to each thread/flow
 */
class ScatterGather {

	private List<Payload> payloads = new ArrayList<>();
	private LinkedBlockingQueue<Payload> toSave = new LinkedBlockingQueue<>();
	private ExecutorService savePool = Executors.newFixedThreadPool(1);

	private final int threads = 50;
	private final int saveQueueLimit = 400;
	private final int pageSize = 1000;

	private int errorCount = 0;
	private int okCount = 0;
	private int saveCount = 0;

	public static class Payload {
		private int number;
		private String string;

		Payload() {
			number = new Random().nextInt(1000);
			string = new RandomString(5).nextString();
		}

		@Override
		public String toString() {
			return number + ":" + string;
		}
	}

	private CompletableFuture<Payload> generateString(int i, ExecutorService x) {
		return CompletableFuture.supplyAsync(() -> {
			// Call flow to fetch from concur
			sleep(500);

			Payload payload = payloads.get(i);

			String result = i + " | " + payload.toString();

			if (payload.number % 10 == 0)
				doError(payload);

			System.out.println(result);

			synchronized (this) {
				okCount++;
			}

			return payload;
		}, x);
	}

	private void doError(Payload payload) {
		System.out.println("Fucked out: " + payload.toString() + "!!!");
		synchronized (this) {
			errorCount++;
		}
		throw new RuntimeException("Error!!!");
	}

	static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			throw new RuntimeException("blah");
		}
	}

	private void addToSaveQueue(Payload result) {
		toSave.add(result);
		save(false);
	}

	private void save(boolean isLast) {
		synchronized (this) {
			if (toSave.size() >= saveQueueLimit || isLast) {
				ArrayList<Payload> saveList = new ArrayList<>();
				toSave.drainTo(saveList, saveQueueLimit);
				if (!saveList.isEmpty())
					CompletableFuture.runAsync(new Saver(saveList, payloads), savePool).thenAccept(s -> {
						synchronized (this) {
							saveCount += saveList.size();
						}
					});
			}
		}
	}

	void go() {

		ExecutorService pool = Executors.newFixedThreadPool(threads);

		System.out.println("Generating list...");
		// Fetch next 1000 reports from potentials service
		for (int i = 0; i < pageSize; i++)
			payloads.add(new Payload());
		System.out.println("List Generation Complete.");

		long startTime = System.nanoTime();
		List<CompletableFuture<Void>> futures = new ArrayList<>();
		for (int i = 0; i < payloads.size(); i++)
			futures.add(generateString(i, pool).thenAccept(this::addToSaveQueue).exceptionally(t -> null));

		System.out.println("Futures Collected.");

		CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]))
			.thenApply(t -> {
					save(true);
					savePool.shutdown();
					try {
						savePool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
						System.out.println(String.format("JOB DONE in %sms. %s failures, %s ok, %s saved.", ((System.nanoTime() - startTime) / 1000000000), errorCount, okCount, saveCount));
					} catch (InterruptedException e) {
						System.out.println("Some exception");
						throw new RuntimeException("Failed to wait", e);
					}

					return t;
				}
			);
	}
}
