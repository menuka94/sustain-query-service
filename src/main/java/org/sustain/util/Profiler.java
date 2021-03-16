package org.sustain.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Profiler {

    public static final Logger log = LogManager.getLogger(Profiler.class);

    private final Map<String, Task> tasks;
    private int indentation;

    public Profiler() {
        this.tasks = new HashMap<>();
    }

    public void addTask(String name) {
        this.tasks.put(name, new Task(name, this.indentation));
    }

    public void indent() {
        ++this.indentation;
    }

    public void unindent() {
        --this.indentation;
    }

    public void completeTask(String name) {
        this.tasks.get(name).finish();
    }

    @Override
    public String toString() {
        List<Task> sortedTasks = new LinkedList<>(tasks.values());
        Collections.sort(sortedTasks);

        StringBuilder sb = new StringBuilder();
        sb.append("\n============= JOB PROFILE ===============\n");
        for (Task task: sortedTasks) {
            sb.append(String.format("%s\n", task.toString()));
        }
        sb.append("=========================================\n");
        return sb.toString();
    }


}
