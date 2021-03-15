package org.sustain.util;

public class Task {

    private Long startTime, endTime;
    private String name;

    public Task(String name) {
        this(name, System.currentTimeMillis());
    }

    public Task(String name, Long startTime) {
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

}
