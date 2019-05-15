package hadoopOperations;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;

import javax.swing.JButton;
import javax.swing.JFrame;

public class Example {
	public static void main(String[] args) throws InterruptedException, IOException {
		JFrame frame = new JFrame();
		frame.setSize(250, 250);
		frame.setVisible(true);

		JButton button = new JButton();
		button.setVisible(true);
		button.setSize(100, 100);
		button.setText("Tıkla");

		frame.add(button);

		button.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				try {
					Runtime.getRuntime().exec(
							"/opt/hadoop-2.9.2/bin/hadoop jar /home/hadoop/Desktop/java-code/minmax_airline.jar hdfs://master:9000/data hdfs://master:9000/output");
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		});
	}
}

/*
	Runtime.getRuntime().exec(
							"/opt/hadoop-2.9.2/bin/hadoop jar /home/hadoop/Desktop/java-code/minmax_airline.jar hdfs://master:9000/data hdfs://master:9000/output");
				
*/