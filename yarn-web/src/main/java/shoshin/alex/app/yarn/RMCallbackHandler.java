package shoshin.alex.app.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import java.io.IOException;
import java.util.List;

class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    private static final Log LOG = LogFactory.getLog(RMCallbackHandler.class);
    private ExecutionManager executionManager;

    public RMCallbackHandler(ExecutionManager executionManager) {
        this.executionManager = executionManager;
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
        LOG.info(completedContainers.size() + " containers was completed");
        for (ContainerStatus containerStatus : completedContainers) {
            executionManager.numCompletedContainers.incrementAndGet();
            LOG.info("Container " + containerStatus.getContainerId() + " completed");
        }
        if (executionManager.numCompletedContainers.get() == executionManager.numRequestedContainers.get()) {
            executionManager.inProgress = false;
        }
    }

    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {
        LOG.info(allocatedContainers.size() + " containers was allocated by RM");
        for (Container allocatedContainer : allocatedContainers) {
            if (executionManager.numAllocatedContainers.get() < executionManager.numRequestedContainers.get()) {
                tryToLaunchContainer(allocatedContainer);
            }
        }
    }
    
    private void tryToLaunchContainer(Container container) {
        LaunchContainerRunnable runnableLaunchContainer;
        try {
            runnableLaunchContainer = new LaunchContainerRunnable(container, executionManager.nmClientAsync, executionManager.executorContainer);
            Thread launchThread = new Thread(runnableLaunchContainer);
            launchThread.start();
        } catch (IOException ex) {
            LOG.error("Fail to launch container: " + ex.getMessage());
        }
    }

    @Override
    public void onShutdownRequest() {
        executionManager.inProgress = false;
    }

    @Override
    public void onNodesUpdated(List<NodeReport> list) {
    }

    @Override
    public float getProgress() {
        return (float) executionManager.numCompletedContainers.get() / executionManager.numRequestedContainers.get();
    }

    @Override
    public void onError(Throwable exc) {
        executionManager.inProgress = false;
        LOG.error(exc.getMessage());
    }
}