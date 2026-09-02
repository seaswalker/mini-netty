package com.github.skywalker.mininetty.worker;

public class ScheduledTask implements Comparable<ScheduledTask> {

    private final long deadline;
    private final Runnable task;

    public ScheduledTask(long delayNanos, Runnable task) {
        this.deadline = System.nanoTime() + delayNanos;
        this.task = task;
    }

    public Runnable getTask() {
        return task;
    }

    public long getDeadline() {
        return deadline;
    }

    @Override
    public int compareTo(ScheduledTask o) {
        return Long.compare(this.deadline, o.deadline);
    }

}
