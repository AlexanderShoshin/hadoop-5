package shoshin.alex.app.yarn;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

class LaunchContainerRunnable implements Runnable {

    private static final Log LOG = LogFactory.getLog(LaunchContainerRunnable.class);
    private Container container;
    private NMClientAsync nmClientAsync;
    private ContainerLaunchContext ctx;

    public LaunchContainerRunnable(Container container, NMClientAsync nmClientAsync, Path executorContainerPath) throws IOException {
        this.nmClientAsync = nmClientAsync;
        this.container = container;
        Map<String, LocalResource> localResources = configurateContainerResources(executorContainerPath);
        List<String> commands = configurateContainerCommands(executorContainerPath.getName());
        Map<String, String> env = setupEnvironment();
        ctx = ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null);
    }
    
    private Map<String, LocalResource> configurateContainerResources(Path executorContainerPath) throws IOException {
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
    
    private List<String> configurateContainerCommands(String jarName) {
        List<CharSequence> vargs = new LinkedList<CharSequence>();
        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
        vargs.add("-jar");
        vargs.add(jarName);
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
        
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        List<String> commands = new ArrayList<String>();
        commands.add(command.toString());
        return commands;
    }
    
    private Map<String, String> setupEnvironment() {
        Map<String, String> env = new HashMap<String, String>();
        Configuration conf = new YarnConfiguration();
        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
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

    @Override
    public void run() {
        LOG.info("Setting up container launch container for containerid=" + container.getId());
        nmClientAsync.startContainerAsync(container, ctx);
    }
}