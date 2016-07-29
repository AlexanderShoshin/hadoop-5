package shoshin.alex.yarn;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import shoshin.alex.yarn.utils.HDFSUtils;

import java.io.IOException;
import java.util.*;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Client {
    private static final Log LOG = LogFactory.getLog(Client.class);
    private static final String APP_NAME = "yarn-application";
    private static final String AM_QUEUE = "default";
    private static final String AM_HDFS_JAR = "AppMaster.jar";
    private static final String CON_HDFS_JAR = "Container.jar";
    private static final boolean KEEP_CONTAINERS = false;
    private static final int AM_PRIORITY = 0;
    private int amMemory = 256;
    private int amCores = 1;
    private String amLocalJar = "";
    private String conLocalJar = "";
    private FileStatus containerRes;
    private Options opts;
    private Configuration conf;
    private YarnClient yarnClient;
    
    public static void main(String[] args) {
        boolean completeSuccessfully = false;
        try {
            LOG.info("Initializing client");
            Client client = new Client();
            try {
                if (!client.init(args)) {
                    System.exit(0);
                }
            } catch (IllegalArgumentException e) {
                client.printUsage();
                System.exit(-1);
            }
            completeSuccessfully = client.run();
        } catch (Throwable exc) {
            System.exit(1);
        }
        if (completeSuccessfully) {
            System.exit(0);
        } else {
            System.exit(2);
        }
    }

    private Client() throws Exception {
        this(new YarnConfiguration());
    }

    private Client(Configuration conf) throws Exception {
        this.conf = conf;
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        opts = new Options();
        opts.addOption("amJar", true, "Jar file containing the application master");
        opts.addOption("conJar", true, "Jar file containing the execution container");
    }
    
    private void printUsage() {
        new HelpFormatter().printHelp("Client", opts);
    }
    
    private boolean init(String[] args) throws ParseException {
        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (!cliParser.hasOption("amJar")) {
            throw new IllegalArgumentException("No jar file specified for application master");
        } else if (!cliParser.hasOption("conJar")) {
            throw new IllegalArgumentException("No jar file specified for execution container");
        }
        amLocalJar = cliParser.getOptionValue("amJar");
        conLocalJar = cliParser.getOptionValue("conJar");

        return true;
    }
    
    private boolean run() throws IOException, YarnException {
        LOG.info("Running Client");
        yarnClient.start();
        
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        
        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        if (amMemory > maxMem) {
            amMemory = maxMem;
        }
        LOG.info(amMemory + " memory will used for AM");

        int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
        if (amCores > maxVCores) {
            amCores = maxVCores;
        }
        LOG.info(amCores + " cores will used for AM");

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();

        appContext.setKeepContainersAcrossApplicationAttempts(KEEP_CONTAINERS);
        appContext.setApplicationName(APP_NAME);
        
        Map<String, LocalResource> localResources = new HashMap<>();
        HDFSUtils hdfsUtils = new HDFSUtils(conf);
        FileStatus resource = hdfsUtils.copyToHDFS(amLocalJar, APP_NAME + "/" + appId + "/" + AM_HDFS_JAR);
        addToLocalResources(resource, localResources);
        containerRes = hdfsUtils.copyToHDFS(conLocalJar, APP_NAME + "/" + appId + "/" + CON_HDFS_JAR);
        addToLocalResources(containerRes, localResources);

        Map<String, String> env = setupEnvironment();
        List<String> commands = setupCommands();
        
        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                localResources, env, commands, null, null, null);

        Resource capability = Resource.newInstance(amMemory, amCores);
        appContext.setResource(capability);
        appContext.setAMContainerSpec(amContainer);
        
        Priority pri = Priority.newInstance(AM_PRIORITY);
        appContext.setPriority(pri);
        appContext.setQueue(AM_QUEUE);
        
        LOG.info("Submitting application to YARN");
        yarnClient.submitApplication(appContext);
        
        return monitorApplication(appId);
    }

    private boolean monitorApplication(ApplicationId appId) throws YarnException, IOException {
        YarnApplicationState lastAppStatus = null;
        FinalApplicationStatus lastFinalStatus = null;
        while (true) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                LOG.debug("sleep interrupded");
            }

            ApplicationReport report = yarnClient.getApplicationReport(appId);
            YarnApplicationState appStatus = report.getYarnApplicationState();
            FinalApplicationStatus finalStatus = report.getFinalApplicationStatus();
            if (lastAppStatus != appStatus || lastFinalStatus != finalStatus) {
                lastAppStatus = appStatus;
                lastFinalStatus = finalStatus;
                LOG.info(String.format("%1$s app status changed: appStatus=%2$s, finalStatus=%3$s",
                                        appId.getId(), appStatus.toString(), finalStatus.toString()));
            }

            if (YarnApplicationState.FINISHED == appStatus) {
                if (FinalApplicationStatus.SUCCEEDED == finalStatus) {
                    LOG.info("Application has completed successfully.");
                    return true;
                } else {
                    LOG.info("Application has completed unsuccessfully.");
                    return false;
                }
            } else if (YarnApplicationState.KILLED == appStatus || YarnApplicationState.FAILED == appStatus) {
                LOG.info("Application was interrupted.");
                return false;
            }
        }
    }

    private void addToLocalResources(FileStatus fileStatus, Map<String, LocalResource> localResources) throws IOException {
        LocalResource resource = LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(fileStatus.getPath().toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        fileStatus.getLen(), fileStatus.getModificationTime());
        localResources.put(fileStatus.getPath().getName(), resource);
    }

    private Map<String, String> setupEnvironment() {
        Map<String, String> env = new HashMap<>();
        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$());
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");

        env.put("CLASSPATH", classPathEnv.toString());
        return env;
    }

    private List<String> setupCommands() {
        List<String> commands = new ArrayList<>();
        List<String> args = new LinkedList<>();

        args.add(Environment.JAVA_HOME.$$() + "/bin/java");
        args.add("-jar");
        args.add(AM_HDFS_JAR);
        args.add(containerRes.getPath().toString());
        args.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        args.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        StringBuilder command = new StringBuilder();
        for (String argument : args) {
            command.append(argument).append(" ");
        }
        commands.add(command.toString());
        
        return commands;
    }
}