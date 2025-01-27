package com.e6.view.display;

import com.e6.dao.EmployerDAO;
import com.e6.dao.InvoiceDAO;
import com.e6.dao.JobDAO;
import com.e6.entity.Employer;
import com.e6.entity.Invoice;
import com.e6.entity.Job;
import com.e6.view.component.SearchBox;
import com.e6.view.helper.Observer;

import javax.swing.*;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class EmployerDisplay extends JPanel implements Observer{

	private static final long serialVersionUID = 8771943950684246172L;

	private final int LLX = 60;
	private final int RLX = 360;
	private final int LLY = 50;
	private final int RLY = 24;
	private final int LLW;
	private final int RLW = 480;
	private final int RLH = 24;
	private final int BS = 20;
	private final int WTH = 180;
	private final int FW = 210;


	private final String[] jobTableColumns = {"ID", "Employer", "Job title", "Description"};
	private final String[] employerPaymentTableColumns = {"ID", "Job title", "Amount", "Date"};


	private JLabel employerImageLabel, employerSearchBoxImageLabel;
	private JLabel jobScrollPaneLabel, employerPaymentScrollPaneLabel, jobCountLabel;
	private JLabel employerPaymentCountLabel, employerPaymentTotalLabel;
	private JLabel employerPaymentJobFilterLabel;
	private JScrollPane jobScrollPane, employerPaymentScrollPane;
	private Employer selectedEmployer;
	private EmployerCard employerCard;
	private SearchBox employerSearchBox, employerPaymentJobFilterSearchBox;
	private Job selectedJob;
	private JButton removeEmployerPaymentJobFilterButton;



	public EmployerDisplay() {
		super();
		setLayout(null);

		LLW = EmployerCard.rowWidth;
		selectedEmployer = null;
		selectedJob = null;

		employerImageLabel = new JLabel(new ImageIcon("src\\icon\\user-profile.png"));
		employerImageLabel.setBounds(LLX + 24, LLY, 128, 128);
		this.add(employerImageLabel);

		employerCard = new EmployerCard();
		employerCard.setBounds(LLX, employerImageLabel.getY() + employerImageLabel.getHeight() + BS, LLW, employerCard.getHeight());
		this.add(employerCard);


		employerSearchBoxImageLabel = new JLabel(new ImageIcon("src\\icon\\search_worker.png"));
		employerSearchBoxImageLabel.setBounds(RLX + (RLW - 64) / 2, RLY, 64, 64);
		this.add(employerSearchBoxImageLabel);

//		employerSearchBox = new SearchBox(employers, new Dimension(RLW, RLH)) {
//			@Override
//			public void mouseAction(MouseEvent e, Object searchResultObject, int chooseIndex) {
//				selectedEmployer = (Employer) searchResultObject;
//				employerCard.setSelectedEmployer(selectedEmployer.clone());
//
//				employerSearchBox.setText(selectedEmployer.toString());
//				employerSearchBox.setEditable(false);
//
//				updateJobTableData();  // Update job table data for the selected employer
//				updateEmployerPaymentTableData();  // Update payment table data for the selected employer
//
//				// Set job filter options based on the selected employer
//				employerPaymentJobFilterSearchBox.setObjectList(JobDAO.getInstance().list(selectedEmployer));
//
//				super.mouseAction(e, searchResultObject, chooseIndex);
//			}
//		};

		employerSearchBox.setBounds(RLX, employerSearchBoxImageLabel.getY() + employerSearchBoxImageLabel.getHeight(), RLW, RLH);
		this.add(employerSearchBox);

		employerSearchBox.getPanel().setBounds(employerSearchBox.getX(), employerSearchBox.getY() + employerSearchBox.getHeight(), RLW, 0);
		this.add(employerSearchBox.getPanel());



		jobScrollPaneLabel = new JLabel("EMPLOYER TABLE");
		jobScrollPaneLabel.setOpaque(true);
		jobScrollPaneLabel.setBackground(new Color(189, 224, 254));
		jobScrollPaneLabel.setHorizontalAlignment(SwingConstants.CENTER);
		jobScrollPaneLabel.setBounds(employerSearchBox.getX(), employerSearchBox.getY() + employerSearchBox.getHeight() + 30, RLW, RLH);
		this.add(jobScrollPaneLabel);

		jobScrollPane = new JScrollPane(new JTable(new String[][] {}, jobTableColumns) {
			private static final long serialVersionUID = 1L;
			public boolean isCellEditable(int row, int column) {
				return false;
			}

			{
				setRowHeight(30);
				setShowVerticalLines(false);
				setShowHorizontalLines(false);
				getColumnModel().getColumn(0).setPreferredWidth(5);
				setAutoResizeMode(JTable.AUTO_RESIZE_LAST_COLUMN);
			}
		});
		jobScrollPane.setBounds(jobScrollPaneLabel.getX(), jobScrollPaneLabel.getY() + jobScrollPaneLabel.getHeight(), RLW, WTH);
		this.add(jobScrollPane);


		jobCountLabel = new JLabel("0 Record");
		jobCountLabel.setBounds(jobScrollPane.getX(), jobScrollPane.getY() + jobScrollPane.getHeight(), RLW, RLH - 8);
		jobCountLabel.setHorizontalAlignment(SwingConstants.CENTER);
		this.add(jobCountLabel);

		employerPaymentScrollPaneLabel = new JLabel("PAYMENTS TABLE");
		employerPaymentScrollPaneLabel.setOpaque(true);
		employerPaymentScrollPaneLabel.setBackground(jobScrollPaneLabel.getBackground());
		employerPaymentScrollPaneLabel.setHorizontalAlignment(SwingConstants.CENTER);
		employerPaymentScrollPaneLabel.setBounds(jobCountLabel.getX(), jobCountLabel.getY() + jobCountLabel.getHeight() + 30, RLW, RLH);
		this.add(employerPaymentScrollPaneLabel);

		employerPaymentScrollPane = new JScrollPane(new JTable(new String[][] {}, employerPaymentTableColumns) {
			private static final long serialVersionUID = 1L;
			public boolean isCellEditable(int row, int column) {
				return false;
			}

			{
				setRowHeight(30);
				setShowVerticalLines(false);
				setShowHorizontalLines(false);
				getColumnModel().getColumn(0).setPreferredWidth(5);
				setAutoResizeMode(JTable.AUTO_RESIZE_LAST_COLUMN);
			}
		});
		employerPaymentScrollPane.setBounds(employerPaymentScrollPaneLabel.getX(), employerPaymentScrollPaneLabel.getY() + employerPaymentScrollPaneLabel.getHeight(), RLW, 150);
		this.add(employerPaymentScrollPane);


		employerPaymentCountLabel = new JLabel("0 Record");
		employerPaymentCountLabel.setBounds(employerPaymentScrollPane.getX() + RLW / 2, employerPaymentScrollPane.getY() + employerPaymentScrollPane.getHeight(), RLW / 2, RLH - 8);
		employerPaymentCountLabel.setHorizontalAlignment(SwingConstants.RIGHT);
		this.add(employerPaymentCountLabel);

		employerPaymentTotalLabel = new JLabel("Total 0 ₺");
		employerPaymentTotalLabel.setBounds(employerPaymentScrollPane.getX(), employerPaymentScrollPane.getY() + employerPaymentScrollPane.getHeight(), RLW / 2, RLH - 8);
		employerPaymentTotalLabel.setHorizontalAlignment(SwingConstants.LEFT);
		this.add(employerPaymentTotalLabel);


		employerPaymentJobFilterLabel = new JLabel("Filter by job");
		employerPaymentJobFilterLabel.setForeground(Color.GRAY);
		employerPaymentJobFilterLabel.setHorizontalAlignment(SwingConstants.CENTER);
		employerPaymentJobFilterLabel.setBounds(employerPaymentScrollPane.getX() + employerPaymentScrollPane.getWidth() + 5, employerPaymentScrollPane.getY(), FW, RLH);
		this.add(employerPaymentJobFilterLabel);

		employerPaymentJobFilterSearchBox = new SearchBox(new ArrayList<Job>(), new Dimension(FW, RLH)) {
			private static final long serialVersionUID = 1L;
			@Override
			public void mouseAction(MouseEvent e, Object searchResultObject, int chooseIndex) {
				selectedJob = (Job) searchResultObject;
				employerPaymentJobFilterSearchBox.setText(selectedJob.toString());
				employerPaymentJobFilterSearchBox.setEditable(false);
				employerPaymentJobFilterLabel.setForeground(new Color(37, 217, 138));
				removeEmployerPaymentJobFilterButton.setVisible(true);
				updateEmployerPaymentTableData();
				super.mouseAction(e, searchResultObject, chooseIndex);
			}
		};
		employerPaymentJobFilterSearchBox.setBounds(employerPaymentJobFilterLabel.getX(), employerPaymentJobFilterLabel.getY() + employerPaymentJobFilterLabel.getHeight(), FW, RLH);
		this.add(employerPaymentJobFilterSearchBox);

		employerPaymentJobFilterSearchBox.getPanel().setBounds(employerPaymentJobFilterSearchBox.getX(), employerPaymentJobFilterSearchBox.getY() + employerPaymentJobFilterSearchBox.getHeight(), employerPaymentJobFilterSearchBox.getWidth(), 0);
		this.add(employerPaymentJobFilterSearchBox.getPanel());

		removeEmployerPaymentJobFilterButton = new JButton("Remove filter");
		removeEmployerPaymentJobFilterButton.setBounds(employerPaymentJobFilterSearchBox.getX(), employerPaymentJobFilterSearchBox.getY() + employerPaymentJobFilterSearchBox.getHeight(), employerPaymentJobFilterSearchBox.getWidth(), 16);
		removeEmployerPaymentJobFilterButton.setBorderPainted(false);
		removeEmployerPaymentJobFilterButton.setFont(new Font(removeEmployerPaymentJobFilterButton.getFont().getName(), Font.BOLD, 9));
		removeEmployerPaymentJobFilterButton.setForeground(Color.WHITE);
		removeEmployerPaymentJobFilterButton.setFocusable(false);
		removeEmployerPaymentJobFilterButton.setBackground(new Color(159, 11, 40));
		removeEmployerPaymentJobFilterButton.setVisible(false);
		removeEmployerPaymentJobFilterButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				removeEmployerPaymentJobFilterButton.setVisible(false);
				employerPaymentJobFilterSearchBox.setText("");
				employerPaymentJobFilterLabel.setForeground(Color.GRAY);
				selectedJob = null;
				updateEmployerPaymentTableData();

			}
		});
		this.add(removeEmployerPaymentJobFilterButton);



	}

	public void updateJobTableData() {

		if(selectedEmployer != null) {

			if (selectedEmployer != null) {
				List<Job> jobList = JobDAO.getInstance().list(selectedEmployer);

				String[][] tableData = new String[jobList.size()][jobTableColumns.length];

				for (int i = 0; i < jobList.size(); i++) {
					Job job = jobList.get(i);
					tableData[i][0] = String.valueOf(job.getId());
					tableData[i][1] = job.getEmployer().toString();  // Assuming job.getEmployer() returns Employer object
					tableData[i][2] = job.getTitle();
					tableData[i][3] = job.getDescription();
				}

				DefaultTableModel model = new DefaultTableModel(tableData, jobTableColumns);
				((JTable) jobScrollPane.getViewport().getView()).setModel(model);
				jobCountLabel.setText(jobList.size() + " Record");
			}
//			((JTable)jobScrollPane.getViewport().getComponent(0)).setModel(new DefaultTableModel(tableData, jobTableColumns));
//			((JTable)jobScrollPane.getViewport().getComponent(0)).getColumnModel().getColumn(0).setPreferredWidth(5);
//			((JTable)jobScrollPane.getViewport().getComponent(0)).setAutoResizeMode(JTable.AUTO_RESIZE_LAST_COLUMN);
//
//			DefaultTableCellRenderer centerRenderer = new DefaultTableCellRenderer();
//			centerRenderer.setHorizontalAlignment( SwingConstants.CENTER );
//			((JTable)jobScrollPane.getViewport().getComponent(0)).getColumnModel().getColumn(0).setCellRenderer(centerRenderer);
//
//			jobCountLabel.setText(jobList.size() + " Kayıt");
		}

	}

	public void updateEmployerPaymentTableData() {

//		if(selectedEmployer != null) {
//
//			List<Invoice> invoiceList = InvoiceDAO.getInstance().list(selectedEmployer);
//			filterPaymentJob(invoiceList);
//
//			String[][] tableData = new String[invoiceList.size()][employerPaymentTableColumns.length];
//			BigDecimal totalAmount = new BigDecimal("0.00");
//			int i = 0;
//			for(Invoice invoice : invoiceList) {
//				tableData[i][0] = invoice.getId() + "";
//				tableData[i][1] = invoice.getJob().toString();
//				tableData[i][2] = invoice.getAmount() + " ₺";
//				tableData[i][3] = new SimpleDateFormat("dd.MM.yyyy").format(invoice.getDate());
//				totalAmount = totalAmount.add(invoice.getAmount());
//				++i;
//			}
//
//			((JTable)employerPaymentScrollPane.getViewport().getComponent(0)).setModel(new DefaultTableModel(tableData, employerPaymentTableColumns));
//			((JTable)employerPaymentScrollPane.getViewport().getComponent(0)).getColumnModel().getColumn(0).setPreferredWidth(5);
//			((JTable)employerPaymentScrollPane.getViewport().getComponent(0)).setAutoResizeMode(JTable.AUTO_RESIZE_LAST_COLUMN);
//
//			DefaultTableCellRenderer centerRenderer = new DefaultTableCellRenderer();
//			centerRenderer.setHorizontalAlignment( SwingConstants.CENTER );
//			((JTable)employerPaymentScrollPane.getViewport().getComponent(0)).getColumnModel().getColumn(0).setCellRenderer(centerRenderer);
//
//			employerPaymentTotalLabel.setText("Total " + totalAmount + " ₺");
//			employerPaymentCountLabel.setText(invoiceList.size() + " Record");
//
//		}
		if (selectedEmployer != null) {
			List<Invoice> invoiceList = InvoiceDAO.getInstance().list(selectedEmployer);

			// Apply job filter if selectedJob is not null
			filterPaymentJob(invoiceList);

			String[][] tableData = new String[invoiceList.size()][employerPaymentTableColumns.length];
			BigDecimal totalAmount = BigDecimal.ZERO;

			for (int i = 0; i < invoiceList.size(); i++) {
				Invoice invoice = invoiceList.get(i);
				tableData[i][0] = String.valueOf(invoice.getId());
				tableData[i][1] = invoice.getJob().toString();  // Assuming invoice.getJob() returns Job object
				tableData[i][2] = invoice.getAmount().toString() + " ₺";  // Assuming invoice.getAmount() returns BigDecimal
				tableData[i][3] = new SimpleDateFormat("dd.MM.yyyy").format(invoice.getDate());
				totalAmount = totalAmount.add(invoice.getAmount());
			}

			DefaultTableModel model = new DefaultTableModel(tableData, employerPaymentTableColumns);
			((JTable) employerPaymentScrollPane.getViewport().getView()).setModel(model);
			employerPaymentTotalLabel.setText("Total " + totalAmount.toString() + " ₺");
			employerPaymentCountLabel.setText(invoiceList.size() + " Record");
		}

	}


	public void filterPaymentJob(List<Invoice> invoiceList) {

		if(selectedJob == null)
			return;

		Iterator<Invoice> invoice = invoiceList.iterator();
		Invoice invoiceItem;
		while(invoice.hasNext()) {
			invoiceItem = invoice.next();
			if(invoiceItem.getJob().getId() != selectedJob.getId()) {
				invoice.remove();
			}
		}

	}


	@Override
	public void update() {
		employerSearchBox.setObjectList(EmployerDAO.getInstance().list());
		selectedEmployer = employerCard.getSelectedEmployer();
		employerSearchBox.setText(selectedEmployer == null ? "" : selectedEmployer.toString());
		JobDAO.getInstance().refresh();
		InvoiceDAO.getInstance().refresh();
		updateJobTableData();
		updateEmployerPaymentTableData();
		employerCard.reload();
	}

}
