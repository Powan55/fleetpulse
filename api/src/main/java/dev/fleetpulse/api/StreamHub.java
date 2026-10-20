package dev.fleetpulse.api;

import dev.fleetpulse.api.Dto.Hello;
import dev.fleetpulse.api.Dto.Items;
import dev.fleetpulse.api.Dto.Resync;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter.DataWithMediaType;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Component
public class StreamHub {
    private static final Logger log = LoggerFactory.getLogger(StreamHub.class);
    private static final Duration WRITE_BUDGET = Duration.ofSeconds(1);
    private static final int MAX_BATCH = 1000;

    private static final class Client {
        final SseEmitter emitter = new SseEmitter(0L);
        final AtomicBoolean writing = new AtomicBoolean();
    }

    private final DeviceRepository devices;
    private final int retryMs;
    private final int maxClients;
    private final String instance = UUID.randomUUID().toString();
    private final Set<Client> clients = ConcurrentHashMap.newKeySet();
    private final Set<String> pending = ConcurrentHashMap.newKeySet();
    private final ExecutorService writers = Executors.newVirtualThreadPerTaskExecutor();

    public StreamHub(DeviceRepository devices, @Value("${fp.sse.retry}") int retryMs,
                     @Value("${fp.sse.max-clients}") int maxClients) {
        this.devices = devices;
        this.retryMs = retryMs;
        this.maxClients = maxClients;
    }

    public SseEmitter register() throws IOException {
        Client c = new Client();
        c.emitter.onCompletion(() -> clients.remove(c));
        c.emitter.onTimeout(() -> clients.remove(c));
        c.emitter.onError(e -> clients.remove(c));
        synchronized (this) {
            if (clients.size() >= maxClients) throw ApiException.unavailable("Too many open streams");
            c.emitter.send(SseEmitter.event().reconnectTime(retryMs).name("hello")
                    .data(new Hello(instance, Instant.now()), MediaType.APPLICATION_JSON));
            clients.add(c);
        }
        return c.emitter;
    }

    public int clientCount() {
        return clients.size();
    }

    public void deviceChanged(Collection<String> ids) {
        pending.addAll(ids);
    }

    public void resync() {
        broadcast(SseEmitter.event().name("resync").data(new Resync("listener_reconnected"), MediaType.APPLICATION_JSON).build());
    }

    @Scheduled(fixedDelayString = "${fp.sse.flush-interval}")
    void flush() {
        Set<String> ids = new HashSet<>();
        for (var it = pending.iterator(); it.hasNext() && ids.size() < MAX_BATCH; ) {
            ids.add(it.next());
            it.remove();
        }
        if (ids.isEmpty() || clients.isEmpty()) return;
        try {
            var items = devices.findByIds(ids);
            if (items.isEmpty()) return;
            Set<DataWithMediaType> msg = new java.util.LinkedHashSet<>();
            msg.addAll(SseEmitter.event().name("devices").data(new Items<>(items), MediaType.APPLICATION_JSON).build());
            msg.addAll(SseEmitter.event().name("fleet.summary").data(devices.summary(), MediaType.APPLICATION_JSON).build());
            broadcast(msg);
        } catch (RuntimeException e) {
            pending.addAll(ids);
            log.warn("sse flush failed, will retry: {}", e.toString());
        }
    }

    @Scheduled(fixedDelayString = "${fp.sse.heartbeat-interval}")
    void heartbeat() {
        broadcast(SseEmitter.event().comment(" hb").build());
    }

    // one task per client so a client that stopped reading cannot hold up the rest
    private void broadcast(Set<DataWithMediaType> msg) {
        List<CompletableFuture<Void>> sent = new ArrayList<>();
        List<Client> started = new ArrayList<>();
        for (Client c : clients) {
            if (!c.writing.compareAndSet(false, true)) {
                drop(c);
                continue;
            }
            started.add(c);
            sent.add(CompletableFuture.runAsync(() -> {
                try {
                    c.emitter.send(msg);
                } catch (IOException | RuntimeException e) {
                    drop(c);
                } finally {
                    c.writing.set(false);
                }
            }, writers));
        }
        try {
            CompletableFuture.allOf(sent.toArray(new CompletableFuture[0])).get(WRITE_BUDGET.toMillis(), TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            for (int i = 0; i < sent.size(); i++) {
                if (!sent.get(i).isDone()) drop(started.get(i));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (java.util.concurrent.ExecutionException e) {
            log.warn("sse broadcast failed: {}", e.toString());
        }
    }

    private void drop(Client c) {
        if (clients.remove(c)) {
            log.info("dropping sse client");
            writers.execute(() -> c.emitter.complete());
        }
    }
}
