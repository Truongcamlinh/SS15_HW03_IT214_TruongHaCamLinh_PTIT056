package com.storex.booking;

import com.storex.events.BookingCreatedEvent;
import com.storex.events.PaymentEvent;
import com.storex.events.SeatEvent;
import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@SpringBootApplication
@RestController
@RequestMapping("/api/bookings")
public class BookingApplication {
    private static final Logger log = LoggerFactory.getLogger(BookingApplication.class);
    private final KafkaTemplate<String, Object> kafka;
    private final Map<String, BookingView> bookings = new ConcurrentHashMap<>();

    public BookingApplication(KafkaTemplate<String, Object> kafka) { this.kafka = kafka; }
    public static void main(String[] args) { SpringApplication.run(BookingApplication.class, args); }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    BookingView create(@RequestBody BookingRequest request) {
        String bookingId = "BKG-" + UUID.randomUUID().toString().substring(0, 8);
        String correlationId = UUID.randomUUID().toString();
        BookingView booking = new BookingView(bookingId, correlationId, request.customerEmail(),
                request.seatNumber(), request.amount(), "PENDING");
        bookings.put(bookingId, booking);
        kafka.send("booking-events", correlationId, new BookingCreatedEvent(correlationId,
                bookingId, request.customerEmail(), request.seatNumber(), request.amount()));
        log.info("[BookingService] correlationId={} - BookingCreated bookingId={}", correlationId, bookingId);
        return booking;
    }

    @GetMapping("/{id}")
    BookingView find(@PathVariable String id) { return bookings.get(id); }

    @KafkaListener(topics = "payment-events", groupId = "booking-group")
    void paymentResult(PaymentEvent event) {
        if ("FAILED".equals(event.status())) update(event.bookingId(), "CANCELLED");
        log.info("[BookingService] correlationId={} - payment status={}", event.correlationId(), event.status());
    }

    @KafkaListener(topics = "seat-events", groupId = "booking-group")
    void seatResult(SeatEvent event) {
        update(event.bookingId(), "RESERVED".equals(event.status()) ? "CONFIRMED" : "CANCELLED");
        log.info("[BookingService] correlationId={} - booking status={}", event.correlationId(),
                bookings.get(event.bookingId()).status());
    }

    private void update(String id, String status) {
        bookings.computeIfPresent(id, (key, old) -> new BookingView(old.bookingId(), old.correlationId(),
                old.customerEmail(), old.seatNumber(), old.amount(), status));
    }

    record BookingRequest(String customerEmail, String seatNumber, BigDecimal amount) {}
    record BookingView(String bookingId, String correlationId, String customerEmail,
                       String seatNumber, BigDecimal amount, String status) {}
}
