package hadoopOperations;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.Font;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.border.Border;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class MainPage {

	private static final String title = "Main Page";
	private static JLabel operation, path_first, path_second;
	private static JTextField field_first, field_second;
	private static JButton doButton;
	private static JTextArea output;
	private JFrame frame;

	private Operations operations;

	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					MainPage window = new MainPage();
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

	public void createFrame() {
		frame = new JFrame();
		Dimension screenDimension = getScreenSize();
		frame.setSize(screenDimension.width / 3, screenDimension.height / 2);
		frame.setLocation(screenDimension.width / 6 + 300, screenDimension.height / 4);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.getContentPane().setLayout(null);
		frame.setTitle(title);
		frame.setResizable(false);
	}

	public MainPage() {
		operations = new Operations();
		createFrame();
		createContinueButton();
		createOutputLabel();

		JMenuBar menuBar = new JMenuBar();
		JMenu menu = new JMenu("Menu");
		JMenuItem mkdir = new JMenuItem("Create Directory");
		mkdir.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				operation.setVisible(true);
				path_first.setVisible(true);
				path_second.setVisible(false);
				field_first.setVisible(true);
				field_second.setVisible(false);
				doButton.setVisible(true);

				operation.setText("Directory creation");
				doButton.setText("Create");
				doButton.setForeground(Color.green);
				doButton.setBounds(110, 170, 100, 30);
				clearButtonActions();

				doButton.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						doButton.setEnabled(false);
						String input = field_first.getText();
						if (input.equals("")) {
							JOptionPane.showMessageDialog(null, "Input can not be empty");
						} else {
							boolean result = operations.mkdir(input);
							if (result) {
								output.setText("Directory " + input + " created");
							} else {
								output.setText("Directory exists");
							}
						}
						doButton.setEnabled(true);
					}
				});
			}
		});

		JMenuItem delete = new JMenuItem("Delete File / Directory");
		delete.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				operation.setVisible(true);
				path_first.setVisible(true);
				path_second.setVisible(false);
				field_first.setVisible(true);
				field_second.setVisible(false);
				doButton.setVisible(true);

				operation.setText("File / Directory deletion");
				doButton.setText("Delete");
				doButton.setForeground(Color.red);
				doButton.setBounds(110, 170, 100, 30);
				clearButtonActions();

				doButton.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						doButton.setEnabled(false);
						String input = field_first.getText();
						if (input.equals("")) {
							JOptionPane.showMessageDialog(null, "Input can not be empty");
						} else {
							boolean result = operations.deleteFileOrDir(input);
							if (result == true) {
								output.setText("File / Directory " + input + " deleted");
							} else {
								output.setText("File / Directory not found");
							}
						}
						doButton.setEnabled(true);
					}
				});
			}
		});

		JMenuItem paths = new JMenuItem("All Paths");
		paths.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				operation.setVisible(true);
				path_first.setVisible(true);
				path_second.setVisible(false);
				field_first.setVisible(true);
				field_second.setVisible(false);
				doButton.setVisible(true);

				operation.setText("List folder");
				doButton.setText("Show");
				doButton.setForeground(Color.blue);
				doButton.setBounds(110, 170, 100, 30);
				clearButtonActions();

				doButton.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						doButton.setEnabled(false);
						String input = field_first.getText();
						if (input.equals("")) {
							JOptionPane.showMessageDialog(null, "Input can not be empty");
						} else {
							Path[] result;
							try {
								result = operations.listFileInGivenPath(input);
								if (result != null) {
									String paths = "";
									for (Path p : result) {
										paths += p.toUri().getRawPath() + "\n";
									}
									output.setText(paths);
								} else {
									output.setText("Directory not found");
								}
							} catch (FileNotFoundException e1) {
								e1.printStackTrace();
							} catch (IllegalArgumentException e1) {
								e1.printStackTrace();
							} catch (IOException e1) {
								e1.printStackTrace();
							}
						}
						doButton.setEnabled(true);
					}
				});
			}
		});

		JMenuItem fileStatus = new JMenuItem("File Status");
		fileStatus.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				operation.setVisible(true);
				path_first.setVisible(true);
				path_second.setVisible(false);
				field_first.setVisible(true);
				field_second.setVisible(false);
				doButton.setVisible(true);

				operation.setText("File status");
				doButton.setText("Show");
				doButton.setForeground(Color.blue);
				doButton.setBounds(110, 170, 100, 30);
				clearButtonActions();

				doButton.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						doButton.setEnabled(false);
						String input = field_first.getText();
						if (input.equals("")) {
							JOptionPane.showMessageDialog(null, "Input can not be empty");
						} else {
							FileStatus result;
							result = operations.getFileStatus(input);
							if (result != null) {
								String status = "Len: " + result.getLen() + "\n" + "Block size: "
										+ result.getBlockSize() + "\n" + "Path: " + result.getPath();
								output.setText(status);
							} else {
								output.setText("File not found");
							}
						}
						doButton.setEnabled(true);
					}
				});
			}
		});

		JMenuItem localToHdfs = new JMenuItem("Local To Hdfs");
		localToHdfs.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				path_first.setVisible(true);
				path_second.setVisible(true);
				field_first.setVisible(true);
				field_second.setVisible(true);
				doButton.setVisible(true);
				operation.setVisible(true);

				operation.setText("Local to hdfs");
				doButton.setText("copy");
				doButton.setForeground(Color.green);
				doButton.setBounds(110, 230, 100, 30);
				clearButtonActions();

				doButton.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						boolean res;
						res = operations.localToHdfs(field_first.getText(), field_second.getText());
						if (res) {
							output.setText("File copied local to hdfs");
						} else {
							output.setText("File not copied");
						}
					}
				});
			}
		});

		JMenuItem hdfsToLocal = new JMenuItem("Hdfs To Local");
		hdfsToLocal.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				path_first.setVisible(true);
				path_second.setVisible(true);
				field_first.setVisible(true);
				field_second.setVisible(true);
				operation.setVisible(true);
				doButton.setVisible(true);

				operation.setText("Hdfs to local");
				doButton.setText("copy");
				doButton.setForeground(Color.green);
				doButton.setBounds(110, 230, 100, 30);
				clearButtonActions();

				doButton.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						boolean res = operations.hdfsToLocal(field_first.getText(), field_second.getText());
						if (res) {
							output.setText("File copied hdfs to local");
						} else {
							output.setText("File not copied");
						}
					}
				});
			}
		});

		JMenuItem blocksInfo = new JMenuItem("File Block Informations");
		blocksInfo.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				path_first.setVisible(true);
				path_second.setVisible(false);
				field_first.setVisible(true);
				field_second.setVisible(false);
				operation.setVisible(true);
				doButton.setVisible(true);

				operation.setText("File blocks informations");
				doButton.setText("show");
				doButton.setForeground(Color.blue);
				doButton.setBounds(110, 170, 100, 30);
				clearButtonActions();

				doButton.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						try {
							BlockLocation[] blocks = operations.getFileBlockLocations(field_first.getText());
							if (blocks != null) {
								String res = "Number of block: " + blocks.length + "\n";
								output.setText(res);
								for (BlockLocation block : blocks) {
									res += "Offset: " + block.getOffset() + " length: " + block.getLength()
											+ " hosts: ";
									String[] hosts;
									hosts = block.getHosts();
									for (String name : hosts) {
										res += name + " ";
									}
									res += "\n";
								}
								System.out.println(res);
							}
						} catch (IOException e1) {
							e1.printStackTrace();
						}
					}
				});
			}
		});

		menu.add(paths);
		menu.add(fileStatus);
		menu.add(localToHdfs);
		menu.add(hdfsToLocal);
		menu.add(mkdir);
		menu.add(delete);
		menu.add(blocksInfo);
		menuBar.add(menu);
		menuBar.setVisible(true);
		frame.setJMenuBar(menuBar);

		createDoButton();
		createOutputTextArea();
		createOperationLabel();
		createFirstTextField();
		createSecondTextField();
	}

	public void createDoButton() {
		doButton = new JButton();
		doButton.setVisible(false);
		doButton.setBounds(110, 170, 100, 30);
		doButton.setFont(new Font("Serif", Font.PLAIN, 12));
		doButton.setBackground(Color.white);
		frame.getContentPane().add(doButton);
	}

	public void createOutputTextArea() {
		output = new JTextArea("");
		output.setBounds(325, 100, 280, 180);
		output.setEditable(false);
		output.setLineWrap(true);
		output.setWrapStyleWord(true);
		Border border = BorderFactory.createLineBorder(Color.BLACK);
		output.setBorder(BorderFactory.createCompoundBorder(border, BorderFactory.createEmptyBorder(10, 10, 10, 10)));
		frame.getContentPane().add(output);
	}

	public void createOperationLabel() {
		operation = new JLabel("Please select an operation in menu");
		operation.setVisible(true);
		operation.setBounds(40, 70, 250, 30);
		operation.setFont(new Font("Serif", Font.PLAIN, 12));
		frame.getContentPane().add(operation);
	}

	public void createFirstTextField() {
		path_first = new JLabel("Path: ");
		path_first.setVisible(false);
		path_first.setBounds(30, 110, 60, 30);
		path_first.setFont(new Font("Serif", Font.PLAIN, 12));

		field_first = new JTextField();
		field_first.setVisible(false);
		field_first.setBounds(70, 110, 200, 30);
		field_first.setFont(new Font("Serif", Font.PLAIN, 12));
		Border border = BorderFactory.createLineBorder(Color.BLACK);
		field_first.setBorder(BorderFactory.createCompoundBorder(border, BorderFactory.createEmptyBorder(0, 5, 0, 0)));

		frame.getContentPane().add(path_first);
		frame.getContentPane().add(field_first);
	}

	public void createSecondTextField() {
		path_second = new JLabel("Path: ");
		path_second.setVisible(false);
		path_second.setBounds(30, 170, 60, 30);
		path_second.setFont(new Font("Serif", Font.PLAIN, 12));

		field_second = new JTextField();
		field_second.setVisible(false);
		field_second.setBounds(70, 170, 200, 30);
		field_second.setFont(new Font("Serif", Font.PLAIN, 12));
		Border border = BorderFactory.createLineBorder(Color.BLACK);
		field_second.setBorder(BorderFactory.createCompoundBorder(border, BorderFactory.createEmptyBorder(0, 5, 0, 0)));

		frame.getContentPane().add(path_second);
		frame.getContentPane().add(field_second);
	}

	public void createContinueButton() {
		JButton continueButton = new JButton();
		continueButton.setBackground(Color.white);
		continueButton.setForeground(Color.green);
		continueButton.setBounds(500, 350, 100, 30);
		continueButton.setText("Continue");
		continueButton.setVisible(true);
		continueButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				getFrame().setVisible(false);
				Functions function = new Functions();
				function.getFrame().setVisible(true);
			}
		});
		frame.getContentPane().add(continueButton);
	}

	public void createOutputLabel() {
		JLabel label = new JLabel("<html>Output</html>");
		label.setFont(new Font("Serif", Font.PLAIN, 12));
		label.setBounds(305, 60, 100, 40);
		frame.getContentPane().add(label);
	}

	public void clearButtonActions() {
		for (ActionListener al : doButton.getActionListeners()) {
			doButton.removeActionListener(al);
		}
	}

	public JFrame getFrame() {
		return frame;
	}

	public void setFrame(JFrame frame) {
		this.frame = frame;
	}
}