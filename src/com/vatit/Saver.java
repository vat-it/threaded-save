package com.vatit;

import java.util.List;

import static com.vatit.ScatterGather.sleep;

public class Saver implements Runnable {

	private final List<ScatterGather.Payload> saveList;
	private final List<?> fullList;

	Saver(List<ScatterGather.Payload> saveList, List<?> fullList) {
		this.saveList = saveList;
		this.fullList = fullList;
	}

	@Override
	public void run() {
		System.out.println(String.format("Starting Save of %s items...", saveList.size(), fullList.size()));
		sleep(1250);
		System.out.println(String.format("Save Complete.", fullList.size()));
	}
}
