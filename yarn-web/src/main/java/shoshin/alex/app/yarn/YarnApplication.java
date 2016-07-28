package shoshin.alex.app.yarn;

import shoshin.alex.app.data.ClasterInfo;
import shoshin.alex.app.ApplicationMaster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import shoshin.alex.app.data.TaskStatus;

/**
 *
 * @author Alexander_Shoshin
 */
public class YarnApplication {
    private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
    public String availableResources;
    AMRMClientAsync amRMClient;
    NMClientAsync nmClientAsync;
    private NMCallbackHandler containerListener;
    private Configuration conf = new YarnConfiguration();
    private RegisterApplicationMasterResponse registrationData;
    private int requestPriority = 10;
    AtomicInteger numTotalContainers = new AtomicInteger(0);
    List<Thread> launchThreads = new ArrayList<Thread>();
    AtomicInteger numCompletedContainers = new AtomicInteger(0);
    volatile boolean inProgress;

    public YarnApplication() throws YarnException, IOException {
        containerListener = new NMCallbackHandler(this);
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, new RMCallbackHandler(this));
        amRMClient.init(conf);
        amRMClient.start();

        String appMasterHostname = NetUtils.getHostname();
        registrationData = amRMClient.registerApplicationMaster(appMasterHostname, -1, "");
    }

    public void startSortingTask(int memoryPerContainer, int coresPerContainer, int numContainers) {
        if (!inProgress) {
            inProgress = true;
            numTotalContainers.set(numContainers);
            numCompletedContainers.set(0);
            for (int i = 0; i < numContainers; ++i) {
                ContainerRequest containerAsk = setupContainerAskForRM(memoryPerContainer, coresPerContainer);
                amRMClient.addContainerRequest(containerAsk);
            }
        }
    }

    private ContainerRequest setupContainerAskForRM(int memoryPerContainer, int coresPerContainer) {
        Priority pri = Priority.newInstance(requestPriority);
        Resource capability = Resource.newInstance(memoryPerContainer, coresPerContainer);
        ContainerRequest request = new ContainerRequest(capability, null, null, pri);
        LOG.info("Requested container: " + request.toString());
        return request;
    }

    public ClasterInfo getInfo() {
        return new ClasterInfo(registrationData.getMaximumResourceCapability().getMemory(),
                               registrationData.getMaximumResourceCapability().getVirtualCores());
    }
    
    public TaskStatus getStatus() {
        return new TaskStatus(numTotalContainers.get(), numCompletedContainers.get(), inProgress);
    }

    public void terminate() {
        LOG.info("Stopping running containers");
        nmClientAsync.stop();
        LOG.info("Signalling finish to RM");
        try {
            amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "terminated", null);
        } catch (YarnException ex) {
            LOG.error("Failed to unregister application", ex);
        } catch (IOException ex) {
            LOG.error("Failed to unregister application", ex);
        }
        amRMClient.stop();
    }
}