package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.apache.camel.main.Main;
import org.apache.camel.support.DefaultMessage;
import org.example.entity.Payment;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Properties;

import static org.apache.camel.builder.Builder.simple;


@Slf4j
public class CamelMain {
    public static void main(String[] args) {
        try {
            Properties properties = getProperties();
            DataSource dataSource = getDataSource(properties);
            final Main main = new Main();
            main.bind("db", dataSource);
            main.configure()
                    .rest()
                    .withHost(properties.getProperty("app.host"))
                    .withPort(Integer.parseInt(properties.getProperty("app.port")))
                    .end()
                    .addLambdaRouteBuilder(builder -> {
                                builder.from("rest://post:order")
                                        .to(String.format("kafka://%s?brokers=%s",
                                                properties.getProperty("kafka.topic"),
                                                properties.getProperty("kafka.brokers")))
                                        .end();
                                builder.from(String.format("kafka://%s?brokers=%s",
                                                properties.getProperty("kafka.topic"),
                                                properties.getProperty("kafka.brokers")))
                                        .unmarshal(new JacksonDataFormat(Payment.class))
                                        .process(CamelMain::prepareMessage)
                                        .setBody(simple("INSERT INTO payment(paymentid, amount, sender, recipient) " +
                                                "VALUES (:?id, :?amount, :?sender, :?recipient)"))
                                        .to("jdbc:db?useHeadersAsParameters=true");
                            }
                    );
            main.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void prepareMessage(Exchange exchange) {
        Message in = exchange.getIn();
        Payment p = (Payment) in.getBody();
        DefaultMessage message = new DefaultMessage(exchange);
        message.setHeaders(in.getHeaders());
        message.setHeader("id", p.getId());
        message.setHeader("amount", p.getAmount());
        message.setHeader("sender", p.getSender());
        message.setHeader("recipient", p.getRecipient());
        exchange.setMessage(message);
    }

    private static DataSource getDataSource(Properties properties) {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setServerNames(new String[]{properties.getProperty("db.host")});
        dataSource.setPortNumbers(new int[]{Integer.parseInt(properties.getProperty("db.port"))});
        dataSource.setDatabaseName(properties.getProperty("db.name"));
        dataSource.setUser(properties.getProperty("db.user"));
        dataSource.setPassword(properties.getProperty("db.password"));
        return dataSource;
    }

    private static Properties getProperties() throws IOException {
        var properties = new Properties();
        properties.load(ClassLoader.getSystemClassLoader().getResourceAsStream("application.properties"));
        return properties;
    }
}