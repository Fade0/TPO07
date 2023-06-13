import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Chat extends JFrame{
    private JTextArea chatView;
    private JPanel mainPanel;
    private JTextField message;
    private JButton sendButton;
    private JButton loginButton;
    private JTextField loginField;
    private JList list1;
    private JTextField passwordField;
    private JButton logoutButton;

    private final MessageConsumer messageConsumer;
    private final JTextField welcomeTextField;
    public Chat(String id, String topic) throws HeadlessException {

        messageConsumer = new MessageConsumer(topic, id);

        message.setEnabled(false);
        message.setText("Type your message");
        sendButton.setEnabled(false);
        chatView.setEnabled(false);

        loginField.setText("Login");
        passwordField.setText("Password");
        welcomeTextField = new JTextField();
        this.setDefaultCloseOperation(EXIT_ON_CLOSE);
        this.add(mainPanel);
        this.setVisible(true);
        this.setTitle(id);
        this.pack();

        Executors.newSingleThreadExecutor().submit(()->{
            while (true) {
                messageConsumer.kafkaConsumer.poll(Duration.of(1, ChronoUnit.SECONDS)).forEach(
                        m -> {
                            System.out.println(m);
                            chatView.append(m.value() + System.lineSeparator());
                        }
                );
            }
        });
        sendButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                MessageProducer.send(new ProducerRecord<>(topic, LocalDateTime.now()+ " - " + id + ": " + message.getText()));
                message.setText("");
            }
        });



        loginButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String username = loginField.getText();
                String pass = passwordField.getText();

                if(username.isEmpty() || pass.isEmpty()){
                    JOptionPane.showMessageDialog(Chat.this, "Check your credentials", "Error", JOptionPane.ERROR_MESSAGE);
                }
                else{
                    String result = getUser(username,pass);
                    MessageProducer.send(new ProducerRecord<>(topic, LocalDateTime.now().format(
                            DateTimeFormatter.ofPattern("HH:MM:SS")) + " " + id + " joined the chat!"));
                    loginButton.setEnabled(false);
                    message.setEnabled(true);
                    sendButton.setEnabled(true);
                    //
                }

            }
        });
        logoutButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                MessageProducer.send(new ProducerRecord<>(topic, LocalDateTime.now().format(
                        DateTimeFormatter.ofPattern("HH:MM:SS")) + " " + id + " left the chat!"));
                loginButton.setEnabled(true);
                message.setEnabled(false);
                sendButton.setEnabled(false);
            }
        });

    }

    public String getUser(String name, String password){
        String result = "";


        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;


        try {
            Class.forName("org.postgresql.ds.PGSimpleDataSource");
            connection = DriverManager.getConnection("jdbc:postgresql://db.tlt.world:5432/pjatk", "pjatk", "efh&*@^T#&f1234");
            String query = "select * from pjatk.users where username = ? and password = ?";
            statement = connection.prepareStatement(query);
            statement.setString(1, name);
            statement.setString(2, password);

            System.out.println(query);
            rs = statement.executeQuery();
            if(rs.next()){
                String username = rs.getString("username");

                welcomeTextField.setText("Hi " + username);
                JOptionPane.showMessageDialog(Chat.this,welcomeTextField,"Hello",JOptionPane.PLAIN_MESSAGE);
            }


        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            welcomeTextField.setText("Error");
            JOptionPane.showMessageDialog(Chat.this,welcomeTextField,"Error",JOptionPane.ERROR_MESSAGE);
        }

        return result;
    }
}
