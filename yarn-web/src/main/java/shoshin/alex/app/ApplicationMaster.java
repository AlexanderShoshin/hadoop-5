package shoshin.alex.app;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import shoshin.alex.app.yarn.ExecutionManager;

import java.io.IOException;
import java.net.URISyntaxException;

@SpringBootApplication
public class ApplicationMaster {
    public static ExecutionManager executionManager;

    public static void main(String[] args) throws IOException, YarnException, URISyntaxException {
        executionManager = new ExecutionManager(args);
        SpringApplication.run(ApplicationMaster.class, args);
    }
}
