package shoshin.alex.yarn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Client {
    private static final Log LOG = LogFactory.getLog(Client.class);
    private static final String APP_NAME = "yarn-application";
    private static final int AM_PRIORITY = 0;
    private static final String AM_QUEUE = "default";
    private static final String AM_HDFS_JAR = "AppMaster.jar";
    private static final String SHELL_COMMAND_PATH = "shellCommands";
    private static final String SHELL_COMMAND = "/bin/date";
    private static final boolean KEEP_CONTAINERS = false;
    private int amMemory = 256;
    private int amCores = 1;
    private String amLocalJar = "";
    private Options opts;
    private Configuration conf;
    private YarnClient yarnClient;
    
    public static void main(String[] args) {
        boolean result = false;
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
            result = client.run();
        } catch (Throwable t) {
            System.exit(1);
        }
        if (result) {
            System.exit(0);
        } else {
            System.exit(2);
        }
    }

    public Client() throws Exception {
        this(new YarnConfiguration());
    }

    public Client(Configuration conf) throws Exception {
        this.conf = conf;
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        opts = new Options();
        opts.addOption("jar", true, "Jar file containing the application master");
    }
    
    private void printUsage() {
        new HelpFormatter().printHelp("Client", opts);
    }
    
    public boolean init(String[] args) throws ParseException {
        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (!cliParser.hasOption("jar")) {
            throw new IllegalArgumentException("No jar file specified for application master");
        }
        amLocalJar = cliParser.getOptionValue("jar");

        return true;
    }
    
    public boolean run() throws IOException, YarnException {
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

        // set local resources for the application master		
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        FileSystem fs = FileSystem.get(conf);
        addToLocalResources(fs, amLocalJar, AM_HDFS_JAR, appId.toString(), localResources, null);


        if (!SHELL_COMMAND.isEmpty()) {
            addToLocalResources(fs, null, SHELL_COMMAND_PATH, appId.toString(), localResources, SHELL_COMMAND);
        }

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
        while (true) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                LOG.debug("sleep interrupded");
            }

            ApplicationReport report = yarnClient.getApplicationReport(appId);
            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus finalStatus = report.getFinalApplicationStatus();
            LOG.debug(String.format("appId=%1$s, yarnAppState=%2$s, distributedFinalState=%3$s",
                                     appId.getId(), state.toString(), finalStatus.toString()));

            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == finalStatus) {
                    LOG.info("Application has completed successfully.");
                    return true;
                } else {
                    LOG.info("Application has completed unsuccessfully.");
                    return false;
                }
            } else if (YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state) {
                LOG.info("Application was interrupted.");
                return false;
            }
        }
    }

    private void addToLocalResources(FileSystem fs, String fileSrcPath,
            String fileDstPath, String appId, Map<String, LocalResource> localResources,
            String resources) throws IOException {
        String suffix = APP_NAME + "/" + appId + "/" + fileDstPath;
        Path dst = new Path(fs.getHomeDirectory(), suffix);
        if (fileSrcPath == null) {
            FSDataOutputStream ostream = null;
            try {
                ostream = FileSystem .create(fs, dst, new FsPermission((short) 0710));
                ostream.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(ostream);
            }
        } else {
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        }
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc = LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(fileDstPath, scRsrc);
    }

    private Map<String, String> setupEnvironment() {
        Map<String, String> env = new HashMap<String, String>();
        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }

        env.put("CLASSPATH", classPathEnv.toString());
        return env;
    }

    private List<String> setupCommands() {
        List<String> commands = new ArrayList<String>();
        List<CharSequence> vargs = new LinkedList<CharSequence>();

        vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
        vargs.add("-jar");
        vargs.add(AM_HDFS_JAR);
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }
        commands.add(command.toString());
        
        return commands;
    }
}