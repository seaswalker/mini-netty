package com.github.skywalker.mininetty.worker;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.github.skywalker.mininetty.handler.Handler;
import com.github.skywalker.mininetty.lifecycle.LifeCycle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A worker that owns its own thread and runs the tasks (e.g. {@link Handler} chain
 * invocations) submitted to it, plus the scheduled tasks due to execute.
 *
 * @author skywalker
 */
public class Worker implements Runnable, LifeCycle {

    private final Queue<Runnable> jobs;
    private final Thread thread;
    private final Lock lock = new ReentrantLock();
    private final Condition jobsFullCondition = lock.newCondition();
    private final Condition jobsEmptyCondition = lock.newCondition();

    private final PriorityQueue<ScheduledTask> scheduledTasks = new PriorityQueue<>(1024);

    private static final int defaultQueueSize = 65536;
    private static final AtomicInteger workerIndex = new AtomicInteger(0);
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);

    private volatile boolean running = true;

    protected Worker() {
        this(defaultQueueSize);
    }

    protected Worker(int queueSize) {
        if (queueSize < 1) {
            queueSize = defaultQueueSize;
        }
        this.jobs = new ArrayDeque<>(queueSize);
        this.thread = new Thread(this, getThreadName());
    }

    @Override
    public void start() {
        thread.start();
    }

    @Override
    public void close() {
        running = false;
        lock.lock();
        try {
            jobsEmptyCondition.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Adds a task to the queue.
     *
     * @param task the task to run
     */
    public void submit(Runnable task) {
        while (true) {
            lock.lock();
            try {
                if (jobs.offer(task)) {
                    jobsEmptyCondition.signal();
                    break;
                }
                jobsFullCondition.await();
            } catch (InterruptedException e) {
                logger.debug("Thread interrupted while submitting task.");
                break;
            } finally {
                lock.unlock();
            }
        }
    }

    public boolean isInWorker() {
        return thread == Thread.currentThread();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void run() {
        while (running) {
            try {
                Runnable job;
                List<ScheduledTask> dueScheduleTasks;
                long waitNanos = -1L;

                lock.lock();
                try {
                    job = jobs.poll();
                    if (job != null) {
                        jobsFullCondition.signal();
                    }
                    dueScheduleTasks = pickDueScheduleTasks();
                    ScheduledTask latestDueScheduleTask;
                    if ((latestDueScheduleTask = scheduledTasks.peek()) != null) {
                        waitNanos = latestDueScheduleTask.getDeadline() - System.nanoTime();
                    }
                } finally {
                    lock.unlock();
                }

                if (job != null) {
                    job.run();
                }

                for (ScheduledTask dueScheduleTask : dueScheduleTasks) {
                    dueScheduleTask.getTask().run();
                }

                if (job == null) {
                    lock.lock();
                    try {
                        if (jobs.isEmpty()) {
                            if (waitNanos > 0L) {
                                jobsEmptyCondition.await(waitNanos, TimeUnit.NANOSECONDS);
                            } else {
                                jobsEmptyCondition.await();
                            }
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                running = false;
                logger.debug("Worker interrupted.");
                break;
            }
        }
    }

    public void schedule(Runnable task, long delay, TimeUnit unit) {
        lock.lock();
        try {
            boolean result = scheduledTasks.offer(new ScheduledTask(unit.toNanos(delay), task));
            if (result) {
                jobsEmptyCondition.signal();
                return;
            }
        } finally {
            lock.unlock();
        }
        logger.debug("Failed to schedule task.");
    }

    private String getThreadName() {
        return String.format("mini-netty-worker-%d", workerIndex.getAndIncrement());
    }

    private List<ScheduledTask> pickDueScheduleTasks() {
        List<ScheduledTask> result = new LinkedList<>();
        ScheduledTask scheduledTask;
        while ((scheduledTask = scheduledTasks.peek()) != null && System.nanoTime() >= scheduledTask.getDeadline()) {
            scheduledTasks.poll();
            result.add(scheduledTask);
        }
        return result;
    }

}
