package org.sustain.util;

import java.time.Duration;

public class Task implements Comparable<Task> {

    private Long startTime, endTime;
    private String name;
    private int indentLevel;

    public Task(String name, int indentLevel) {
        this(name, System.currentTimeMillis(), indentLevel);
    }

    public Task(String name, Long startTime, int indentLevel) {
        this.startTime = startTime;
        this.name = name;
        this.indentLevel = indentLevel;
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

    public int getIndentLevel() {
        return this.indentLevel;
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
        return minutes == 0 ? String.format("%d S", seconds) : String.format("%d M %d S", minutes, seconds);
    }

    private String indent() {
        return "  ".repeat(this.indentLevel);
    }

    @Override
    public String toString() {
        return String.format("%s{ %s : %s }", indent(), this.name, timeToEnglish());
    }

    @Override
    public int compareTo(Task task) {
        if (this.startTime < task.getStartTime()) {
            return -1;
        } else if (this.startTime > task.getStartTime()) {
            return 1;
        }
        return 0; // This would be weird if two tasks started at the same time
    }
}
