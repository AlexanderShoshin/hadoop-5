package shoshin.alex.app.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.util.*;

class LaunchContainerRunnable implements Runnable {
    private static final Log LOG = LogFactory.getLog(LaunchContainerRunnable.class);
    private Container container;
    private NMClientAsync nmClientAsync;
    private ContainerLaunchContext containerContext;

    LaunchContainerRunnable(Container container, NMClientAsync nmClientAsync, Path executorContainerPath) throws IOException {
        this.nmClientAsync = nmClientAsync;
        this.container = container;
        Map<String, LocalResource> localResources = setupContainerResources(executorContainerPath);
        List<String> commands = setupContainerCommands(executorContainerPath.getName());
        Map<String, String> env = setupEnvironment();
        containerContext = ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null);
    }
    
    private Map<String, LocalResource> setupContainerResources(Path executorContainerPath) throws IOException {
        FileSystem fs = FileSystem.get(new YarnConfiguration());
        FileStatus executorContainer = fs.getFileStatus(new Path("Container.jar"));
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        addToLocalResources(executorContainer, localResources);
        fs.close();
        return localResources;
    }
    
    private void addToLocalResources(FileStatus fileStatus, Map<String, LocalResource> localResources) throws IOException {
        LocalResource resource = LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(fileStatus.getPath().toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        fileStatus.getLen(), fileStatus.getModificationTime());
        localResources.put(fileStatus.getPath().getName(), resource);
    }
    
    private List<String> setupContainerCommands(String jarName) {
        List<String> args = new LinkedList<>();
        args.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
        args.add("-jar");
        args.add(jarName);
        args.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        args.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
        
        StringBuilder command = new StringBuilder();
        for (String argument: args) {
            command.append(argument).append(" ");
        }
        List<String> commands = new ArrayList<>();
        commands.add(command.toString());
        return commands;
    }
    
    private Map<String, String> setupEnvironment() {
        Map<String, String> env = new HashMap<>();
        Configuration conf = new YarnConfiguration();
        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$());
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String path: conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                          YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(path.trim());
        }
        env.put("CLASSPATH", classPathEnv.toString());
        return env;
    }

    @Override
    public void run() {
        LOG.info("Starting container " + container.getId());
        nmClientAsync.startContainerAsync(container, containerContext);
    }
}