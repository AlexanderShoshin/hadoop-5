package shoshin.alex.yarn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ExecutionContainer {
    private static final Integer count = 10_000_000;
    public static void main(String[] args) throws IOException {
        sortRandomNumbers();
    }
    private static void sortRandomNumbers() {
        List<Integer> listToSort = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            listToSort.add((int)(Math.random() * count));
        }
        Collections.sort(listToSort);
    }
}