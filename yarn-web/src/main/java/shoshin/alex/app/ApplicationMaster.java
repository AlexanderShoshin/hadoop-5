package shoshin.alex.app;

import java.io.IOException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import shoshin.alex.app.yarn.YarnApplication;

@SpringBootApplication
public class ApplicationMaster {

    public static YarnApplication yarnSetup;

    public static void main(String[] args) throws IOException, YarnException {
        yarnSetup = new YarnApplication();
        SpringApplication.run(ApplicationMaster.class, args);
    }
}
