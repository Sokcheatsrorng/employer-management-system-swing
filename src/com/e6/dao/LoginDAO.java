package com.e6.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class LoginDAO {
	
	public boolean verifyLogin(String username, String pass) {
		
		Connection conn;
		Statement st;
		ResultSet rs;
	
		String query = "SELECT id FROM auth WHERE username='" + username + "' AND password='" + pass + "';";
		
		try {
			conn = DB.getConnection();
			st = conn.createStatement();
			rs = st.executeQuery(query);
			
			if(rs.next())
				return true;
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
	}
	
}
