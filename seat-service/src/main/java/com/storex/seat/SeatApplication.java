package com.storex.seat;

import com.storex.events.PaymentEvent;
import com.storex.events.SeatEvent;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class SeatApplication {
    private static final Logger log = LoggerFactory.getLogger(SeatApplication.class);
    private final KafkaTemplate<String, Object> kafka;
    private final Set<String> reservedSeats = ConcurrentHashMap.newKeySet();

    public SeatApplication(KafkaTemplate<String, Object> kafka) { this.kafka = kafka; }
    public static void main(String[] args) { SpringApplication.run(SeatApplication.class, args); }

    @KafkaListener(topics = "payment-events", groupId = "seat-group")
    void reserve(PaymentEvent event) {
        if (!"SUCCESS".equals(event.status())) return;
        boolean reserved = event.seatNumber() != null && !event.seatNumber().startsWith("X")
                && reservedSeats.add(event.seatNumber());
        SeatEvent result = new SeatEvent(event.correlationId(), event.bookingId(), event.customerEmail(),
                event.seatNumber(), event.paymentId(), reserved ? "RESERVED" : "FAILED",
                reserved ? null : "Ghế không còn khả dụng");
        kafka.send("seat-events", event.correlationId(), result);
        log.info("[SeatService] correlationId={} - seat {} status={}", event.correlationId(),
                event.seatNumber(), result.status());
    }
}
