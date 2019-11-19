package MovieInfoCrawl;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Proxy {
    String ip;
    int port;
    AtomicInteger retryTime = new AtomicInteger(10);
    AtomicLong lastUse = new AtomicLong(System.currentTimeMillis());
    Proxy(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public int getRetryTime() {
        return retryTime.get();
    }

    public int decreaseAndGetRetryTime() {
        return this.retryTime.decrementAndGet();
    }

    @Override
    public String toString(){
        return ip + ":" + port;
    }
}