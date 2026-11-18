package cl.duoc.digitalfix.report;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Reporteria: KPIs en tiempo real desde una proyeccion alimentada por Kafka. */
@SpringBootApplication
@EnableScheduling
public class ReportApplication {
    public static void main(String[] args) {
        SpringApplication.run(ReportApplication.class, args);
    }
}
