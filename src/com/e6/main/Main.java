package com.e6.main;

import java.awt.*;
import java.util.Locale;

public class Main {
	
	public static void main(String[] args) {

		Locale.setDefault(new Locale("en", "US"));

		EventQueue.invokeLater(new Runnable() {
			
			@Override
			public void run() {
				
				new Login();
				
			}
		});
			
	}
	
}
