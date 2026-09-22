package com.storex.payment;

import com.storex.events.BookingCreatedEvent;
import com.storex.events.PaymentEvent;
import com.storex.events.SeatEvent;
import java.math.BigDecimal;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class PaymentApplication {
    private static final Logger log = LoggerFactory.getLogger(PaymentApplication.class);
    private final KafkaTemplate<String, Object> kafka;
    private final Set<String> refundedPayments = ConcurrentHashMap.newKeySet();

    public PaymentApplication(KafkaTemplate<String, Object> kafka) { this.kafka = kafka; }
    public static void main(String[] args) { SpringApplication.run(PaymentApplication.class, args); }

    @KafkaListener(topics = "booking-events", groupId = "payment-group")
    void pay(BookingCreatedEvent event) {
        boolean accepted = event.amount() != null && event.amount().compareTo(BigDecimal.valueOf(5_000_000)) <= 0;
        String paymentId = accepted ? "PAY-" + UUID.randomUUID().toString().substring(0, 8) : null;
        PaymentEvent result = new PaymentEvent(event.correlationId(), event.bookingId(),
                event.customerEmail(), event.seatNumber(), paymentId,
                accepted ? "SUCCESS" : "FAILED", accepted ? null : "Số tiền vượt hạn mức mô phỏng");
        kafka.send("payment-events", event.correlationId(), result);
        log.info("[PaymentService] correlationId={} - payment status={}", event.correlationId(), result.status());
    }

    @KafkaListener(topics = "seat-events", groupId = "payment-compensation-group")
    void compensate(SeatEvent event) {
        if ("FAILED".equals(event.status()) && event.paymentId() != null && refundedPayments.add(event.paymentId())) {
            log.warn("[PaymentService] correlationId={} - refunded paymentId={} because seat failed",
                    event.correlationId(), event.paymentId());
        }
    }
}
