package org.garret.bench;

public abstract class Test implements Runnable {
	boolean completed;
	
	public synchronized void done() { 
		completed = true;
		notify();
	}
	
	public abstract String getName();
}
