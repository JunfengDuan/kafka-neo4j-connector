package org.kafka.neo4j;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@PropertySource({
        "classpath:application.properties",
        "classpath:config/kafka.properties",
        "classpath:config/neo4j.properties"
}
)

public class KafkaNeo4jConnectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaNeo4jConnectorApplication.class, args);
    }
}
