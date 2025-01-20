package org.example;
import java.io.Serializable;

public class AggregateState implements Serializable {
    private int sum;
    private int count;

    public AggregateState() {
        this.sum = 0;
        this.count = 0;
    }

    public AggregateState add(int value) {
        this.sum += value;
        this.count++;
        return this;
    }

    public double calculateMean() {
        return count == 0 ? 0 : (double) sum / count;
    }

    public String toString() {
        return sum + "," + count;
    }

    public static AggregateState fromString(String stateStr) {
        String[] parts = stateStr.split(",");
        AggregateState state = new AggregateState();
        state.sum = Integer.parseInt(parts[0]);
        state.count = Integer.parseInt(parts[1]);
        return state;
    }
}
