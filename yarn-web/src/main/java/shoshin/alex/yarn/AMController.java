package shoshin.alex.yarn;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AMController {
    @RequestMapping("/ping")
    public String testConnection() {
        return "{\"status\": \"pong\"}";
    }
    
    @RequestMapping("/terminate")
    public void terminateApplication() {
        System.exit(0);
    }
    
    @RequestMapping("/sort")
    public void sort() {
    }
}