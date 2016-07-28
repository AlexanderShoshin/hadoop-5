package shoshin.alex.app.yarn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

/**
 *
 * @author Alexander_Shoshin
 */
class LaunchContainerRunnable implements Runnable {

    private static final Log LOG = LogFactory.getLog(LaunchContainerRunnable.class);
    private Container container;
    private NMClientAsync nmClientAsync;
    //private NMCallbackHandler containerListener;

    public LaunchContainerRunnable(Container container,
            //NMCallbackHandler containerListener,
            NMClientAsync nmClientAsync) {
        this.nmClientAsync = nmClientAsync;
        this.container = container;
       // this.containerListener = containerListener;
    }

    @Override
    public void run() {
        LOG.info("Setting up container launch container for containerid=" + container.getId());

        // Set the local resources
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        // Set the necessary command to execute on the allocated container
        List<CharSequence> vargs = new LinkedList<CharSequence>();
        vargs.add("/bin/date");
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        List<String> commands = new ArrayList<String>();
        commands.add(command.toString());

        ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                localResources, null, commands, null, null, null);
        //containerListener.addContainer(container.getId(), container);
        nmClientAsync.startContainerAsync(container, ctx);
    }
}