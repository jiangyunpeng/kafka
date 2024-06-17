package com.buzz.kafka.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 可复用的CountDown
 * Author: bairen
 * Date:2024/4/23 11:28
 */
public class CountDown {
    private CountDownLatch countDownLatch;
    private int count;

    public CountDown(int count) {
        this.count = count;
        this.countDownLatch = new CountDownLatch(count);
    }

    public void countDown() {
        countDownLatch.countDown();
    }

    public void await() throws InterruptedException {
        countDownLatch.await();
    }

    public void await(long timeout, TimeUnit unit) throws InterruptedException {
        countDownLatch.await(timeout,unit);
    }

    public void reset() {
        this.countDownLatch = new CountDownLatch(count);
    }
}
