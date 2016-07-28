package shoshin.alex.app.yarn;

import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

/**
 *
 * @author Alexander_Shoshin
 */
class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    private static final Log LOG = LogFactory.getLog(RMCallbackHandler.class);
    private YarnApplication yarnApp;

    public RMCallbackHandler(YarnApplication yarnSetup) {
        this.yarnApp = yarnSetup;
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
        LOG.info(completedContainers.size() + " containers was completed");
        for (ContainerStatus containerStatus : completedContainers) {
            yarnApp.numCompletedContainers.incrementAndGet();
            LOG.info("Container " + containerStatus.getContainerId() + " completed");
        }
        if (yarnApp.numCompletedContainers.get() == yarnApp.numTotalContainers.get()) {
            yarnApp.inProgress = false;
        }
    }

    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {
        LOG.info(allocatedContainers.size() + " containers was allocated by RM");
        for (Container allocatedContainer : allocatedContainers) {
            LaunchContainerRunnable runnableLaunchContainer
                    = new LaunchContainerRunnable(allocatedContainer,
                            //containerListener,
                            yarnApp.nmClientAsync);
            Thread launchThread = new Thread(runnableLaunchContainer);
            yarnApp.launchThreads.add(launchThread);
            launchThread.start();
        }
    }

    @Override
    public void onShutdownRequest() {
        yarnApp.inProgress = false;
    }

    @Override
    public void onNodesUpdated(List<NodeReport> list) {
    }

    @Override
    public float getProgress() {
        return (float) 0.5;
    }

    @Override
    public void onError(Throwable thrwbl) {
        yarnApp.inProgress = false;
    }
}
