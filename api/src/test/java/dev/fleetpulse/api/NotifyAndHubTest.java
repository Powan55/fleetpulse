package dev.fleetpulse.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

class NotifyAndHubTest {
    @Test
    void acceptsDeviceAndAlertPayloads() {
        assertEquals(List.of("dev-0001", "dev-0002"), ChangeListener.parse("{\"t\":\"d\",\"ids\":[\"dev-0001\",\"dev-0002\"]}").devices());
        assertEquals(List.of(), ChangeListener.parse("{\"t\":\"a\",\"ids\":[17,18]}").devices());
    }

    @Test
    void dropsBadPayloads() {
        String[] bad = {
            null, "", "not json", "[]", "{}", "{\"t\":\"d\"}", "{\"t\":\"d\",\"ids\":[]}",
            "{\"t\":\"x\",\"ids\":[\"dev-0001\"]}", "{\"t\":\"d\",\"ids\":[\"dev-1\"]}", "{\"t\":\"d\",\"ids\":[\"dev-0001'; --\"]}",
            "{\"t\":\"d\",\"ids\":[1]}", "{\"t\":\"a\",\"ids\":[\"x\"]}", "{\"t\":\"a\",\"ids\":[0]}",
            "{\"t\":\"d\",\"t\":\"a\",\"ids\":[1]}", "{\"t\":\"d\",\"ids\":[\"dev-0001\"],\"extra\":1}",
            "{\"t\":\"d\",\"ids\":[" + "\"dev-0001\",".repeat(200) + "\"dev-0001\"]}",
            "{\"t\":\"d\",\"ids\":[\"dev-0001\"]}" + " ".repeat(8000),
        };
        for (String b : bad) assertNull(ChangeListener.parse(b), String.valueOf(b));
        assertNotNull(ChangeListener.parse("{\"t\":\"d\",\"ids\":[\"dev-0001\"]}"));
    }

    @Test
    void capsOpenStreams() throws Exception {
        StreamHub hub = new StreamHub(null, 2000, 2);
        hub.register();
        hub.register();
        ApiException e = assertThrows(ApiException.class, hub::register);
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, e.status);
        assertEquals(2, hub.clientCount());
    }
}
