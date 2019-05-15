package hadoopOperations;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.Font;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.border.Border;
import javax.swing.table.DefaultTableModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;

public class Functions {

	private static final String master = "hdfs://master:9000/";
	private static final String title = "Functions Page";
	private FileSystem fileSystem;
	private JScrollPane scrollPane;
	private JFrame frame;
	private JTable table;

	private Operations operations;
	private String[] columns;

	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					Functions window = new Functions();
					window.frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	public Dimension getScreenSize() {
		Toolkit toolkit = Toolkit.getDefaultToolkit();
		Dimension screenSize = toolkit.getScreenSize();
		return screenSize;
	}

	public Functions() {
		frame = new JFrame();
		Dimension screenDimension = getScreenSize();
		frame.setSize(screenDimension.width / 3, screenDimension.height / 2);
		frame.setLocation(screenDimension.width / 6 + 300, screenDimension.height / 4);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.getContentPane().setLayout(null);
		frame.setTitle(title);
		frame.setResizable(false);

		operations = new Operations();

		JLabel inputLabel = new JLabel();
		inputLabel.setText("input : ");
		inputLabel.setBounds(40, 30, 60, 40);
		inputLabel.setFont(new Font("Serif", Font.PLAIN, 12));
		JLabel outputLabel = new JLabel();
		outputLabel.setText("output : ");
		outputLabel.setBounds(40, 90, 60, 40);
		outputLabel.setFont(new Font("Serif", Font.PLAIN, 12));
		final JTextField inputField = new JTextField();
		inputField.setBounds(110, 35, 130, 30);
		Border border = BorderFactory.createLineBorder(Color.BLACK);
		inputField.setBorder(BorderFactory.createCompoundBorder(border, BorderFactory.createEmptyBorder(5, 5, 5, 5)));
		final JTextField outputField = new JTextField();
		outputField.setBounds(110, 95, 130, 30);
		outputField.setBorder(BorderFactory.createCompoundBorder(border, BorderFactory.createEmptyBorder(5, 5, 5, 5)));
		JLabel functions = new JLabel();
		functions.setText("Functions");
		functions.setFont(new Font("Serif", Font.PLAIN, 15));
		functions.setBounds(80, 140, 100, 30);
		functions.setForeground(Color.BLUE);
		frame.getContentPane().add(inputLabel);
		frame.getContentPane().add(outputLabel);
		frame.getContentPane().add(inputField);
		frame.getContentPane().add(outputField);
		frame.getContentPane().add(functions);

		JLabel result = new JLabel();
		result.setText("   Result   ");
		result.setFont(new Font("Serif", Font.PLAIN, 14));
		result.setBounds(350, 100, 100, 30);
		result.setForeground(Color.red);
		frame.getContentPane().add(result);

		scrollPane = new JScrollPane();
		scrollPane.setBounds(325, 140, 250, 150);
		frame.getContentPane().add(scrollPane);

		table = new JTable();
		scrollPane.setViewportView(table);
		table.setColumnSelectionAllowed(true);
		table.setCellSelectionEnabled(true);

		final JButton averageFlightCancellation = new JButton("Average Cancellation");
		final JButton maxFlight = new JButton("Max Flight");
		final JButton minMaxAirline = new JButton("Min Max Airline");
		final JButton standardDeviationTaxi = new JButton("Standard Deviation Taxi");
		final JButton varienceDepartureDelay = new JButton("Varience Departure Delay");

		averageFlightCancellation.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				maxFlight.setEnabled(false);
				minMaxAirline.setEnabled(false);
				standardDeviationTaxi.setEnabled(false);
				varienceDepartureDelay.setEnabled(false);
				frame.setTitle("Map - Reduce task running...");
				try {
					if (!inputField.getText().equals("") && !outputField.getText().equals("")) {
						operations.deleteFileOrDir(outputField.getText());
						String[] args = new String[] { master + inputField.getText(), master + outputField.getText() };
						AverageFlightCancellation.main(args);
						columns = new String[] { "Year", "Cancellation Rate" };
						createTable(master + outputField.getText(), columns);
					} else {
						JOptionPane.showMessageDialog(null, "Input or output is empty");
					}
				} catch (InvalidInputException e2) {
					JOptionPane.showMessageDialog(null, "Input path does not exist");
				} catch (FileAlreadyExistsException e1) {
					JOptionPane.showMessageDialog(null, "Output file already exist");
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				frame.setTitle(title);
				maxFlight.setEnabled(true);
				minMaxAirline.setEnabled(true);
				standardDeviationTaxi.setEnabled(true);
				varienceDepartureDelay.setEnabled(true);
			}
		});
		averageFlightCancellation.setBounds(40, 180, 195, 30);
		averageFlightCancellation.setBackground(Color.white);
		averageFlightCancellation.setFont(new Font("Serif", Font.PLAIN, 12));
		frame.getContentPane().add(averageFlightCancellation);

		maxFlight.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				averageFlightCancellation.setEnabled(false);
				minMaxAirline.setEnabled(false);
				standardDeviationTaxi.setEnabled(false);
				varienceDepartureDelay.setEnabled(false);
				frame.setTitle("Map - Reduce task running...");
				try {
					if (!inputField.getText().equals("") && !outputField.getText().equals("")) {
						operations.deleteFileOrDir(outputField.getText());
						String[] args = new String[] { master + inputField.getText(), master + outputField.getText() };
						MaxFlightMonthYearDuo.main(args);
						columns = new String[] { "Year / Month", "Count" };
						createTable(master + outputField.getText(), columns);
					} else {
						JOptionPane.showMessageDialog(null, "Input or output is empty");
					}
				} catch (InvalidInputException e2) {
					JOptionPane.showMessageDialog(null, "Input path does not exist");
				} catch (FileAlreadyExistsException e1) {
					JOptionPane.showMessageDialog(null, "Output file already exist");
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				frame.setTitle(title);
				averageFlightCancellation.setEnabled(true);
				minMaxAirline.setEnabled(true);
				standardDeviationTaxi.setEnabled(true);
				varienceDepartureDelay.setEnabled(true);
			}
		});
		maxFlight.setBounds(40, 230, 195, 30);
		maxFlight.setBackground(Color.white);
		maxFlight.setFont(new Font("Serif", Font.PLAIN, 12));
		frame.getContentPane().add(maxFlight);

		minMaxAirline.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				averageFlightCancellation.setEnabled(false);
				maxFlight.setEnabled(false);
				standardDeviationTaxi.setEnabled(false);
				varienceDepartureDelay.setEnabled(false);
				frame.setTitle("Map - Reduce task running...");
				try {
					if (!inputField.getText().equals("") && !outputField.getText().equals("")) {
						operations.deleteFileOrDir(outputField.getText());
						String[] args = new String[] { master + inputField.getText(), master + outputField.getText() };
						MinMaxAirline.main(args);
						columns = new String[] { "Airline", "Count" };
						createTable(master + outputField.getText(), columns);
					} else {
						JOptionPane.showMessageDialog(null, "Input or output is empty");
					}
				} catch (InvalidInputException e2) {
					JOptionPane.showMessageDialog(null, "Input path does not exist");
				} catch (FileAlreadyExistsException e1) {
					JOptionPane.showMessageDialog(null, "Output file already exist");
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				frame.setTitle(title);
				averageFlightCancellation.setEnabled(true);
				maxFlight.setEnabled(true);
				standardDeviationTaxi.setEnabled(true);
				varienceDepartureDelay.setEnabled(true);
			}
		});
		minMaxAirline.setBounds(40, 280, 195, 30);
		minMaxAirline.setBackground(Color.white);
		minMaxAirline.setFont(new Font("Serif", Font.PLAIN, 12));
		frame.getContentPane().add(minMaxAirline);

		standardDeviationTaxi.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				averageFlightCancellation.setEnabled(false);
				maxFlight.setEnabled(false);
				minMaxAirline.setEnabled(false);
				varienceDepartureDelay.setEnabled(false);
				frame.setTitle("Map - Reduce task running...");
				try {
					if (!inputField.getText().equals("") && !outputField.getText().equals("")) {
						operations.deleteFileOrDir(outputField.getText());
						String[] args = new String[] { master + inputField.getText(), master + outputField.getText() };
						StandardDeviationTaxi.main(args);
						columns = new String[] { "Output String", "Standart Deviation" };
						createTable(master + outputField.getText(), columns);
					} else {
						JOptionPane.showMessageDialog(null, "Input or output is empty");
					}
				} catch (InvalidInputException e2) {
					JOptionPane.showMessageDialog(null, "Input path does not exist");
				} catch (FileAlreadyExistsException e1) {
					JOptionPane.showMessageDialog(null, "Output file already exist");
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				frame.setTitle(title);
				averageFlightCancellation.setEnabled(true);
				maxFlight.setEnabled(true);
				minMaxAirline.setEnabled(true);
				varienceDepartureDelay.setEnabled(true);
			}
		});
		standardDeviationTaxi.setBounds(40, 330, 195, 30);
		standardDeviationTaxi.setBackground(Color.white);
		standardDeviationTaxi.setFont(new Font("Serif", Font.PLAIN, 12));
		frame.getContentPane().add(standardDeviationTaxi);

		varienceDepartureDelay.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				averageFlightCancellation.setEnabled(false);
				maxFlight.setEnabled(false);
				minMaxAirline.setEnabled(false);
				standardDeviationTaxi.setEnabled(false);
				frame.setTitle("Map - Reduce task running...");
				try {
					if (!inputField.getText().equals("") && !outputField.getText().equals("")) {
						operations.deleteFileOrDir(outputField.getText());
						String[] args = new String[] { master + inputField.getText(), master + outputField.getText() };
						VarienceDepartureDelay.main(args);
						columns = new String[] { "Output String", "Varience" };
						createTable(master + outputField.getText(), columns);
					} else {
						JOptionPane.showMessageDialog(null, "Input or output is empty");
					}
				} catch (InvalidInputException e2) {
					JOptionPane.showMessageDialog(null, "Input path does not exist");
				} catch (FileAlreadyExistsException e1) {
					JOptionPane.showMessageDialog(null, "Output file already exist");
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				frame.setTitle(title);
				averageFlightCancellation.setEnabled(true);
				maxFlight.setEnabled(true);
				minMaxAirline.setEnabled(true);
				standardDeviationTaxi.setEnabled(true);
			}
		});
		varienceDepartureDelay.setBounds(40, 380, 195, 30);
		varienceDepartureDelay.setBackground(Color.white);
		varienceDepartureDelay.setFont(new Font("Serif", Font.PLAIN, 12));
		frame.getContentPane().add(varienceDepartureDelay);

		JButton back = new JButton("Back");
		back.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				getFrame().setVisible(false);
				MainPage mainPage = new MainPage();
				mainPage.getFrame().setVisible(true);
			}
		});
		back.setBounds(500, 380, 100, 30);
		back.setBackground(Color.white);
		back.setFont(new Font("Serif", Font.PLAIN, 12));
		frame.getContentPane().add(back);

		configurationForReadingOutputFile();
	}

	public void configurationForReadingOutputFile() {
		Configuration configuration = new Configuration();
		try {
			URI uri = new URI("hdfs://master:9000");
			fileSystem = (DistributedFileSystem) FileSystem.get(uri, configuration);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createTable(String output, String[] columns) {
		DefaultTableModel tableModel = (DefaultTableModel) table.getModel();
		tableModel.setRowCount(0);

		int colCount = columns.length;
		tableModel.setColumnCount(colCount);
		tableModel.setColumnIdentifiers(columns);

		BufferedReader bufferedReader;
		output += "/part-r-00000";
		try {
			bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(output))));
			String line;
			String[] cols;
			while ((line = bufferedReader.readLine()) != null) {
				if (columns.length == 2)
					cols = line.split("\t");
				else {
					line.trim();
					cols = line.split(",");
				}
				tableModel.addRow(cols);
			}
			bufferedReader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public JFrame getFrame() {
		return frame;
	}

	public void setFrame(JFrame frame) {
		this.frame = frame;
	}
}