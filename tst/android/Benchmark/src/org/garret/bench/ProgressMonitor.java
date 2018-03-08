package org.garret.bench;

import android.widget.*;
import android.os.Handler;
import java.io.*;

public class ProgressMonitor implements Runnable {
	Test test;
	ByteArrayOutputStream result;
	long start;
	TextView view;
	Handler handler;
	
	ProgressMonitor(Test test, TextView view, ByteArrayOutputStream result)
	{
		this.test = test;
		this.view = view;
		this.result = result;
		start = System.currentTimeMillis();
		handler = new Handler();
	}
	
	public void run() { 
		try { 
			synchronized (test) { 
				while (!test.completed) {
					handler.post(new Runnable() {
						public void run() {
							view.setText("Running test for " + test.getName() + "\nElapsed time: " + (System.currentTimeMillis() - start)/1000 + " sec");
							view.invalidate();
						}
					});
					test.wait(10000);
				}
				handler.post(new Runnable() {
					public void run() {
						view.setText(result.toString());
						view.invalidate();
					}
				});
			}
		} catch (Exception x) {
			System.out.println("Exception caught: " + x);
		}
	}
}
