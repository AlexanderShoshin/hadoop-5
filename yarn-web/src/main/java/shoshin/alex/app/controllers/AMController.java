package shoshin.alex.app.controllers;

import org.springframework.web.bind.annotation.RequestParam;
import shoshin.alex.app.ApplicationMaster;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import shoshin.alex.app.data.ClasterInfo;
import shoshin.alex.app.data.TaskStatus;

@RestController
public class AMController {
    @RequestMapping("/getInfo")
    public ClasterInfo getClasterInfo() {
        return ApplicationMaster.yarnSetup.getInfo();
    }
    
    @RequestMapping("/getTaskStatus")
    public TaskStatus getTaskStatus() {
        return ApplicationMaster.yarnSetup.getStatus();
    }
    
    @RequestMapping(value="/terminate", method = RequestMethod.POST)
    public void terminateApplication() {
        ApplicationMaster.yarnSetup.terminate();
        System.exit(0);
    }
    
    @RequestMapping(value="/startSorting", method = RequestMethod.GET)
    public void startSortint(@RequestParam int memory, @RequestParam int cores, @RequestParam int containers) {
        ApplicationMaster.yarnSetup.startSortingTask(memory, cores, containers);
    }
}