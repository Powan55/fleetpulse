package dev.fleetpulse.api;

import dev.fleetpulse.api.Dto.DevicePage;
import dev.fleetpulse.api.Dto.FleetSummary;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import dev.fleetpulse.api.Dto.DeviceDetail;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
public class FleetController {
    private static final Pattern DEVICE_ID = Pattern.compile("^dev-[0-9]{4,6}$");
    private final DeviceRepository devices;

    public FleetController(DeviceRepository devices) {
        this.devices = devices;
    }

    @GetMapping("/fleet/summary")
    FleetSummary summary() {
        return devices.summary();
    }

    @GetMapping("/devices/{deviceId}")
    DeviceDetail device(@PathVariable String deviceId,
                        @RequestParam(defaultValue = "450") int telemetryLimit,
                        @RequestParam(defaultValue = "50") int historyLimit) {
        List<ApiException.FieldError> errors = new ArrayList<>();
        if (!DEVICE_ID.matcher(deviceId).matches()) errors.add(new ApiException.FieldError("deviceId", "must match ^dev-[0-9]{4,6}$"));
        if (telemetryLimit < 1 || telemetryLimit > 450) errors.add(new ApiException.FieldError("telemetryLimit", "must be between 1 and 450"));
        if (historyLimit < 1 || historyLimit > 200) errors.add(new ApiException.FieldError("historyLimit", "must be between 1 and 200"));
        if (!errors.isEmpty()) throw ApiException.validation(errors);
        return devices.detail(deviceId, telemetryLimit, historyLimit).orElseThrow(() -> ApiException.notFound("Device not found"));
    }

    @GetMapping("/devices")
    DevicePage devices(@RequestParam(required = false) String state,
                       @RequestParam(defaultValue = "0") int page,
                       @RequestParam(defaultValue = "100") int size) {
        List<ApiException.FieldError> errors = new ArrayList<>();
        if (state != null && !DeviceRepository.STATES.contains(state)) {
            errors.add(new ApiException.FieldError("state", "must be one of " + DeviceRepository.STATES));
        }
        if (page < 0 || page > 10000) errors.add(new ApiException.FieldError("page", "must be between 0 and 10000"));
        if (size < 1 || size > 1000) errors.add(new ApiException.FieldError("size", "must be between 1 and 1000"));
        if (!errors.isEmpty()) throw ApiException.validation(errors);
        return devices.list(state, page, size);
    }
}
