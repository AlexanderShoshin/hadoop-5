package shoshin.alex.app.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import java.nio.ByteBuffer;
import java.util.Map;

class NMCallbackHandler implements NMClientAsync.CallbackHandler {
    private static final Log LOG = LogFactory.getLog(NMCallbackHandler.class);
    private ExecutionManager executionManager;
    
    NMCallbackHandler(ExecutionManager executionManager) {
        this.executionManager = executionManager;
    }
    
    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> map) {
        LOG.debug("Succeeded to start container " + containerId);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
        LOG.debug("Container status: id=" + containerId + ", status=" + containerStatus);
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
        LOG.debug("Succeeded to stop Container " + containerId);
    }

    @Override
    public void onStartContainerError(ContainerId id, Throwable exc) {
        executionManager.numCompletedContainers.incrementAndGet();
        LOG.info("Container " + id + " was not started");
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable exc) {
        LOG.error("Failed to query the status of container " + containerId);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable exc) {
        LOG.error("Failed to stop container " + containerId);
    }   
}