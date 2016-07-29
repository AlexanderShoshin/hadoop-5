package shoshin.alex.app.controllers;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import shoshin.alex.app.ApplicationMaster;
import shoshin.alex.app.data.ClusterInfo;
import shoshin.alex.app.data.TaskStatus;

@RestController
public class AMController {
    @RequestMapping("/getInfo")
    public ClusterInfo getClusterInfo() {
        return ApplicationMaster.executionManager.getInfo();
    }
    
    @RequestMapping("/getTaskStatus")
    public TaskStatus getTaskStatus() {
        return ApplicationMaster.executionManager.getStatus();
    }
    
    @RequestMapping(value="/terminate", method = RequestMethod.POST)
    public void terminateApplication() {
        ApplicationMaster.executionManager.terminate();
        System.exit(0);
    }
    
    @RequestMapping(value="/startTasks", method = RequestMethod.GET)
    public void startTasks(@RequestParam int memory, @RequestParam int cores, @RequestParam int containers) {
        ApplicationMaster.executionManager.launchTask(memory, cores, containers);
    }
}