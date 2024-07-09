package com.e6.dao;


import com.e6.entity.Employer;
import com.e6.exception.EntityException;

import javax.swing.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class EmployerDAO {

	private final HashMap<Integer, Employer> cache = new HashMap<>();
	private boolean usingCache = true;

	private EmployerDAO() {list();}

	public Employer findById(int id) {

		if(usingCache == false)
			list();

		if(cache.containsKey(id))
			return cache.get(id);
		return null;
	}


	public List<Employer> list() {
		List<Employer> list = new ArrayList<>();
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;

		try {
			conn = DB.getConnection();
			if (conn == null) {
				JOptionPane.showMessageDialog(null, "Failed to connect to the database.");
				return list;
			}

			st = conn.createStatement();
			rs = st.executeQuery("SELECT * FROM employer");

			while (rs.next()) {
				Employer.EmployerBuilder builder = new Employer.EmployerBuilder()
						.setId(rs.getInt("id"))
						.setFname(rs.getString("fname"))
						.setLname(rs.getString("lname"))
						.setTel(rs.getArray("tel") == null ? null : Arrays.asList((String[]) rs.getArray("tel").getArray()))
						.setDescription(rs.getString("description"))
						.setDate(rs.getTimestamp("date"));

				try {
					Employer employer = builder.build();
					list.add(employer);
					cache.put(employer.getId(), employer);

				} catch (EntityException e) {
					showEntityException(e, rs.getString("fname") + " " + rs.getString("lname"));
				}
			}

			if (list.isEmpty()) {
				JOptionPane.showMessageDialog(null, "No employers found.");
			}

		} catch (SQLException e) {
			showSQLException(e);
		} finally {
			closeResources(rs, st, conn);
		}

		return list;
	}

	private void closeResources(ResultSet rs, Statement st, Connection conn) {
		try {
			if (rs != null) {
				rs.close();
			}
			if (st != null) {
				st.close();
			}
			if (conn != null) {
				conn.close();
			}
		} catch (SQLException e) {
			showSQLException(e);
		}
	}

	private void showEntityException(EntityException e, String msg) {
		String message = msg + " not added\n" + e.getMessage() + "\n" + e.getLocalizedMessage() + e.getCause();
		JOptionPane.showMessageDialog(null, message);
	}

	private void showSQLException(SQLException e) {
		String message = e.getErrorCode() + "\n" + e.getMessage() + "\n" + e.getLocalizedMessage() + "\n" + e.getCause();
		JOptionPane.showMessageDialog(null, message);
	}


	public boolean create(Employer employer) {

		if(createControl(employer) == false)
			return false;

		Connection conn;
		PreparedStatement pst;
		int result = 0;
		String query = "INSERT INTO employer (fname,lname,tel,description) VALUES (?,?,?,?);";
		String query2 = "SELECT * FROM employer ORDER BY id DESC LIMIT 1;";

		try {
			conn = DB.getConnection();
			pst = conn.prepareStatement(query);
			pst.setString(1, employer.getFname());
			pst.setString(2, employer.getLname());

			if(employer.getTel() == null)
				pst.setArray(3, null);
			else {
				Array phones = conn.createArrayOf("VARCHAR", employer.getTel().toArray());
				pst.setArray(3, phones);
			}

			pst.setString(4, employer.getDescription());
			result = pst.executeUpdate();

			// adding cache
			if(result != 0) {

				ResultSet rs = conn.createStatement().executeQuery(query2);
				while(rs.next()) {

					Employer.EmployerBuilder builder = new Employer.EmployerBuilder();
					builder.setId(rs.getInt("id"));
					builder.setFname(rs.getString("fname"));
					builder.setLname(rs.getString("lname"));

					if(rs.getArray("tel") == null)
						builder.setTel(null);
					else
						builder.setTel(Arrays.asList((String [])rs.getArray("tel").getArray()));

					builder.setDescription(rs.getString("description"));
					builder.setDate(rs.getTimestamp("date"));

					try {

						Employer emp = builder.build();
						cache.put(emp.getId(), emp);

					} catch (EntityException e) {
						showEntityException(e, rs.getString("fname") + " " + rs.getString("lname"));
					}

				}

			}

		} catch (SQLException e) {
			showSQLException(e);
		}

		return result == 0 ? false : true;
	}

	private boolean createControl(Employer employer) {

		for(Entry<Integer, Employer> obj : cache.entrySet()) {
			if(obj.getValue().getFname().equals(employer.getFname())
					&& obj.getValue().getLname().equals(employer.getLname())) {

				DB.ERROR_MESSAGE = obj.getValue().getFname() + " " + obj.getValue().getLname() + " kaydı zaten mevcut.";
				return false;
			}
		}

		return true;
	}


	public boolean update(Employer employer) {

		if(updateControl(employer) == false)
			return false;

		Connection conn;
		PreparedStatement pst;
		int result = 0;
		String query = "UPDATE employer SET fname=?,"
				+ "lname=?, tel=?, description=? WHERE id=?;";

		try {
			conn = DB.getConnection();
			pst = conn.prepareStatement(query);
			pst.setString(1, employer.getFname());
			pst.setString(2, employer.getLname());

			Array phones = conn.createArrayOf("VARCHAR", employer.getTel().toArray());
			pst.setArray(3, phones);

			pst.setString(4, employer.getDescription());
			pst.setInt(5, employer.getId());

			result = pst.executeUpdate();

			// update cache
			if(result != 0) {
				cache.put(employer.getId(), employer);
			}

		} catch (SQLException e) {
			showSQLException(e);
		}

		return result == 0 ? false : true;
	}

	private boolean updateControl(Employer employer) {
		for(Entry<Integer, Employer> obj : cache.entrySet()) {
			if(obj.getValue().getFname().equals(employer.getFname())
					&& obj.getValue().getLname().equals(employer.getLname())
					&& obj.getValue().getId() != employer.getId()) {
				DB.ERROR_MESSAGE = obj.getValue().getFname() + " " + obj.getValue().getLname() + " kaydı zaten mevcut.";
				return false;
			}
		}
		return true;
	}

	public boolean delete(Employer employer) {

		Connection conn;
		PreparedStatement ps;
		int result = 0;
		String query = "DELETE FROM employer WHERE id=?;";

		try {

			conn = DB.getConnection();
			ps = conn.prepareStatement(query);
			ps.setInt(1, employer.getId());
			result = ps.executeUpdate();

			if(result != 0) {
				cache.remove(employer.getId());
			}


		} catch (SQLException e) {
			showSQLException(e);
		}

		return result == 0 ? false : true;

	}


	public boolean isUsingCache() {
		return this.usingCache;
	}

	public void setUsingCache(boolean usingCache) {
		this.usingCache = usingCache;
	}

	private static class EmployerDAOHelper{
		private static final EmployerDAO instance = new EmployerDAO();
	}

	public static EmployerDAO getInstance() {
		return EmployerDAOHelper.instance;
	}


}
