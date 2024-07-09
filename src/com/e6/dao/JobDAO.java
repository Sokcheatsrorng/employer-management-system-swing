package com.e6.dao;

import com.e6.entity.Employer;
import com.e6.entity.Job;
import com.e6.entity.Job.JobBuilder;
import com.e6.exception.EntityException;

import javax.swing.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class JobDAO {
	
	private final HashMap<Integer, Job> cache = new HashMap<>();
	private boolean usingCache = true;
	
	private JobDAO() {list();}
	
	// Read by id
	public Job findById(int id) {
		
		if(usingCache == false)
			list();
		if(cache.containsKey(id))
			return cache.get(id);
		return null;
		
	}
	
	public void refresh() {
		setUsingCache(false);
		list();
		setUsingCache(true);
	}

	public List<Job> list(Employer employer) {
		List<Job> jobList = new ArrayList<>();
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		String query = "SELECT * FROM job WHERE employer_id = ?";

		try {
			conn = DB.getConnection();
			pst = conn.prepareStatement(query);
			pst.setInt(1, employer.getId());
			rs = pst.executeQuery();

			while (rs.next()) {
				JobBuilder builder = new JobBuilder();
				builder.setId(rs.getInt("id"));
				builder.setEmployer_id(rs.getInt("employer_id"));
				builder.setPrice_id(rs.getInt("price_id"));
				builder.setTitle(rs.getString("title"));
				builder.setDescription(rs.getString("description"));
				builder.setDate(rs.getTimestamp("date"));

				try {
					Job job = builder.build();
					jobList.add(job);
					cache.put(job.getId(), job);
				} catch (EntityException e) {
					showEntityException(e, "Error while building job");
				}
			}
		} catch (SQLException e) {
			showSQLException(e);
		} finally {
			try {
				if (rs != null) rs.close();
				if (pst != null) pst.close();
				if (conn != null) conn.close();
			} catch (SQLException e) {
				showSQLException(e);
			}
		}

		return jobList;
	}



	//Read All
	public List<Job> list(){
		
		List<Job> list = new ArrayList<>();
		
		if(cache.size() != 0 && usingCache) {
			for(Entry<Integer, Job> obj : cache.entrySet()) {
				list.add(obj.getValue());
			}
			return list;
		}
		
		cache.clear();
		
		Connection conn;
		Statement st;
		ResultSet rs;
		String query = "SELECT * FROM job;";
		
		try {
			conn = DB.getConnection();
			st = conn.createStatement();
			rs = st.executeQuery(query);
			
			JobBuilder builder;
			Job job;
			
			while(rs.next()) {
				
				builder = new JobBuilder();
				builder.setId(rs.getInt("id"));
				builder.setEmployer_id(rs.getInt("employer_id"));
				builder.setPrice_id(rs.getInt("price_id"));
				builder.setTitle(rs.getString("title"));
				builder.setDescription(rs.getString("description"));
				builder.setDate(rs.getTimestamp("date"));
				
				try {
					job = builder.build();
					list.add(job);
					cache.put(job.getId(), job);
				} catch (EntityException e) {
					showEntityException(e, "ID : " + rs.getInt("id") + " Title : " + rs.getString("title"));
				}
				
			}
			
		} catch (SQLException e) {
			showSQLException(e);
		}
		
		return list;
	}
	

	
	private boolean createControl(Job job) {
		
		for(Entry<Integer, Job> obj : cache.entrySet()) {
			if(obj.getValue().getTitle().equals(job.getTitle())) {
				DB.ERROR_MESSAGE = obj.getValue().getTitle() ;
				return false;
			}
		}
		return true;
	}

	public boolean create(Job job) {
		if (!createControl(job)) {
			return false;
		}

		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		String insertQuery = "INSERT INTO job (employer_id, price_id, title, description) VALUES (?, ?, ?, ?)";
		String selectQuery = "SELECT * FROM job WHERE id = (SELECT MAX(id) FROM job)";

		try {
			conn = DB.getConnection();
			pst = conn.prepareStatement(insertQuery);
			pst.setInt(1, job.getEmployer().getId());
			pst.setInt(2, job.getPrice().getId());
			pst.setString(3, job.getTitle());
			pst.setString(4, job.getDescription());

			int result = pst.executeUpdate();

			if (result > 0) {
				// Retrieve the last inserted job
				pst = conn.prepareStatement(selectQuery);
				rs = pst.executeQuery();

				if (rs.next()) {
					JobBuilder builder = new JobBuilder();
					builder.setId(rs.getInt("id"));
					builder.setEmployer_id(rs.getInt("employer_id"));
					builder.setPrice_id(rs.getInt("price_id"));
					builder.setTitle(rs.getString("title"));
					builder.setDescription(rs.getString("description"));
					builder.setDate(rs.getTimestamp("date"));

					try {
						Job insertedJob = builder.build();
						cache.put(insertedJob.getId(), insertedJob);
						return true; // Return true upon successful insertion
					} catch (EntityException e) {
						showEntityException(e, "Error while building inserted job");
					}
				}
			}

		} catch (SQLException e) {
			showSQLException(e);
		} finally {
			try {
				if (rs != null) rs.close();
				if (pst != null) pst.close();
				if (conn != null) conn.close();
			} catch (SQLException e) {
				showSQLException(e);
			}
		}

		return false; // Return false if the insertion or retrieval failed
	}




	public boolean update(Job job) {
		
		if(updateControl(job) == false)
			return false;
		
		Connection conn;
		PreparedStatement pst;
		int result = 0;
		String query = "UPDATE job SET employer_id=?,"
				+ "price_id=?, title=?, description=? WHERE id=?;";
		
		try {
			conn = DB.getConnection();
			pst = conn.prepareStatement(query);
			pst.setInt(1, job.getEmployer().getId());
			pst.setInt(2, job.getPrice().getId());
			pst.setString(3, job.getTitle());
			pst.setString(4, job.getDescription());
			pst.setInt(5, job.getId());
			
			result = pst.executeUpdate();
			
			if(result != 0) {
				cache.put(job.getId(), job);
			}
			
		} catch (SQLException e) {
			showSQLException(e);
		}
		
		return result == 0 ? false : true;
	}
	
	private boolean updateControl(Job job) {
		for(Entry<Integer, Job> obj : cache.entrySet()) {
			if(obj.getValue().getTitle().equals(job.getTitle()) && obj.getValue().getId() != job.getId()) {
				DB.ERROR_MESSAGE = obj.getValue().getTitle();
				return false;
			}
		}
		return true;
	}
	
	
	public boolean delete(Job job) {
		
		Connection conn;
		PreparedStatement ps;
		int result = 0;
		String query = "DELETE FROM job WHERE id=?;";
		
		try {
			
			conn = DB.getConnection();
			ps = conn.prepareStatement(query);
			ps.setInt(1, job.getId());
			
			result = ps.executeUpdate();
			
			if(result != 0) {
				cache.remove(job.getId());
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

	
	private static class JobDAOHelper {
		private static final JobDAO instance = new JobDAO();
	}
	
	public static JobDAO getInstance() {
		return JobDAOHelper.instance;
	}
	
	private void showEntityException(EntityException e, String msg) {
		String message = msg + " not added" + 
				"\n" + e.getMessage() + "\n" + e.getLocalizedMessage() + e.getCause();
			JOptionPane.showMessageDialog(null, message);
	}
	
	private void showSQLException(SQLException e) {
		String message = e.getErrorCode() + "\n" + e.getMessage() + "\n" + e.getLocalizedMessage() + "\n" + e.getCause();
		JOptionPane.showMessageDialog(null, message);
	}
}
