package shoshin.alex.app.data;

public class TaskStatus {
    private int containersTotal;
    private int containersCompleted;
    private boolean inProgress;

    public TaskStatus(int containersTotal, int containersCompleted, boolean inProgress) {
        this.containersTotal = containersTotal;
        this.containersCompleted = containersCompleted;
        this.inProgress = inProgress;
    }

    public int getContainersTotal() {
        return containersTotal;
    }

    public int getContainersCompleted() {
        return containersCompleted;
    }

    public boolean isInProgress() {
        return inProgress;
    }
}