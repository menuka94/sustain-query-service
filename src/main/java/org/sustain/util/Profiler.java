package org.sustain.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sustain.handlers.DirectQueryHandler;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class Profiler {

    public static final Logger log = LogManager.getLogger(Profiler.class);

    private Map<String, Task> tasks;

    public Profiler() {
        this.tasks = new HashMap<>();
    }

    public void addTask(String name) {
        log.info("PROFILER: Starting task: {}", name);
        addTask(new Task(name));
    }

    public void addTask(Task task) {
        this.tasks.put(task.getName(), task);
    }

    public Task getTask(String name) {
        return tasks.get(name);
    }

    public void completeTask(String name) {
        this.tasks.get(name).finish();
        log.info("PROFILER: {}", timeToEnglish(name));
        this.tasks.remove(name);
    }

    public Long timeTaken(Task task) {
        return task.getEndTime() - task.getStartTime();
    }

    /**
     * Solution taken from StackOverflow:
     * https://stackoverflow.com/questions/17624335/converting-milliseconds-to-minutes-and-seconds
     * @return English representation of time
     */
    public String timeToEnglish(String name) {
        Duration d = Duration.ofMillis(timeTaken(tasks.get(name))) ;
        int minutes = d.toMinutesPart();
        int seconds = d.toSecondsPart();

        return minutes == 0 ? String.format("%s: took %d seconds", name, seconds) :
                String.format("%s: took %d minutes %d seconds", name, minutes, seconds);
    }


}
