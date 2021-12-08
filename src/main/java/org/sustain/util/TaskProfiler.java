package org.sustain.util;

import java.time.Duration;

public class TaskProfiler implements Comparable<TaskProfiler> {

    private final Long startTime;
    private Long endTime;
    private final String name;

    public TaskProfiler(String name) {
        this(name, System.currentTimeMillis());
    }

    public TaskProfiler(String name, Long startTime) {
        this.startTime = startTime;
        this.name = name;
    }

    public void finish() {
        this.endTime = System.currentTimeMillis();
    }

    public Long getStartTime() {
        return startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public String getName() {
        return name;
    }

    public Long timeTaken() {
        return this.endTime - this.startTime;
    }

    /**
     * Solution taken from StackOverflow:
     * https://stackoverflow.com/questions/17624335/converting-milliseconds-to-minutes-and-seconds
     * @return English representation of time
     */
    public String timeToEnglish() {
        Duration d = Duration.ofMillis(timeTaken()) ;
        int minutes = d.toMinutesPart();
        int seconds = d.toSecondsPart();
        return minutes == 0 ? String.format("%d sec", seconds) : String.format("%d min %d sec", minutes, seconds);
    }

    @Override
    public String toString() {
        return String.format("{ %s : %s }", this.name, timeToEnglish());
    }

    @Override
    public int compareTo(TaskProfiler other) {
        if (this.startTime < other.getStartTime()) {
            return -1;
        } else if (this.startTime > other.getStartTime()) {
            return 1;
        }
        return 0; // Unlikely that two tasks started at the same time in ms
    }
}
