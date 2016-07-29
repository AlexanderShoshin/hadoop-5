package shoshin.alex.app.data;

public class ClusterInfo {
    private final int maxMem;
    private final int maxVCores;
    
    public ClusterInfo(int maxMem, int maxVCores) {
        this.maxMem = maxMem;
        this.maxVCores = maxVCores;
    }
    
    public int getMaxMem() {
        return maxMem;
    }

    public int getMaxVCores() {
        return maxVCores;
    }
}
