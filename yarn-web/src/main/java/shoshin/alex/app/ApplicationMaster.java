package shoshin.alex.app;

import java.io.IOException;
import java.net.URISyntaxException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import shoshin.alex.app.yarn.YarnApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;

@SpringBootApplication
public class ApplicationMaster {

    public static YarnApplication yarnSetup;

    public static void main(String[] args) throws IOException, YarnException, URISyntaxException {
        yarnSetup = new YarnApplication(args);
        SpringApplication.run(ApplicationMaster.class, args);
    }
}
