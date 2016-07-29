package shoshin.alex.app.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import shoshin.alex.app.ApplicationMaster;
import shoshin.alex.app.data.ClusterInfo;
import shoshin.alex.app.data.TaskStatus;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecutionManager {
    private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
    private static final int REQUEST_PRIORITY = 0;
    private static final int HEARTBEAT_INTERVAL = 1000;
    private RegisterApplicationMasterResponse registrationData;
    private AMRMClientAsync amRMClient;
    NMClientAsync nmClientAsync;
    AtomicInteger numRequestedContainers = new AtomicInteger(0);
    AtomicInteger numAllocatedContainers = new AtomicInteger(0);
    AtomicInteger numCompletedContainers = new AtomicInteger(0);
    volatile boolean inProgress;
    Path executorContainer;
    

    public ExecutionManager(String[] args) throws YarnException, URISyntaxException, IOException {
        parseInputParams(args);
        setupYarnClients();
        collectRegistrationData();
    }

    private void parseInputParams(String[] args) {
        if (args.length != 0) {
            executorContainer = new Path(args[0]);
        } else {
            throw new IllegalArgumentException("Executor container path missed");
        }
    }

    private void setupYarnClients() {
        Configuration conf = new YarnConfiguration();
        nmClientAsync = new NMClientAsyncImpl(new NMCallbackHandler(this));
        nmClientAsync.init(conf);
        nmClientAsync.start();

        amRMClient = AMRMClientAsync.createAMRMClientAsync(HEARTBEAT_INTERVAL, new RMCallbackHandler(this));
        amRMClient.init(conf);
        amRMClient.start();
    }

    private void collectRegistrationData() throws IOException, YarnException {
        String appMasterHostname = NetUtils.getHostname();
        registrationData = amRMClient.registerApplicationMaster(appMasterHostname, -1, "");
    }

    public void launchTask(int memoryPerContainer, int coresPerContainer, int numContainers) {
        if (!inProgress) {
            inProgress = true;
            numRequestedContainers.set(numContainers);
            numCompletedContainers.set(0);
            numAllocatedContainers.set(0);
            for (int i = 0; i < numContainers; ++i) {
                ContainerRequest containerAsk = setupContainerAskForRM(memoryPerContainer, coresPerContainer);
                amRMClient.addContainerRequest(containerAsk);
            }
        }
    }

    private ContainerRequest setupContainerAskForRM(int memoryPerContainer, int coresPerContainer) {
        Priority pri = Priority.newInstance(REQUEST_PRIORITY);
        Resource capability = Resource.newInstance(memoryPerContainer, coresPerContainer);
        ContainerRequest request = new ContainerRequest(capability, null, null, pri);
        LOG.info("Requested container: " + request.toString());
        return request;
    }

    public ClusterInfo getInfo() {
        return new ClusterInfo(registrationData.getMaximumResourceCapability().getMemory(),
                               registrationData.getMaximumResourceCapability().getVirtualCores());
    }
    
    public TaskStatus getStatus() {
        return new TaskStatus(numRequestedContainers.get(), numCompletedContainers.get(), inProgress);
    }

    public void terminate() {
        LOG.info("Stopping running containers");
        nmClientAsync.stop();
        LOG.info("Signalling finish to RM");
        try {
            amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "terminated", null);
        } catch (YarnException | IOException ex) {
            LOG.error("Failed to unregister application", ex);
        }
        amRMClient.stop();
    }
}