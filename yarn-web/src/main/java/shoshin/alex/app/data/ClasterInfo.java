package shoshin.alex.app.data;

/**
 *
 * @author Alexander_Shoshin
 */
public class ClasterInfo {
    private final int maxMem;
    private final int maxVCores;
    
    public ClasterInfo(int maxMem, int maxVCores) {
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
