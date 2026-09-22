package com.storex.notification;

import com.storex.events.SeatEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
public class NotificationApplication {
    private static final Logger log = LoggerFactory.getLogger(NotificationApplication.class);
    public static void main(String[] args) { SpringApplication.run(NotificationApplication.class, args); }

    @KafkaListener(topics = "seat-events", groupId = "notification-group")
    void handleSeatReserved(SeatEvent event) {
        if (!"RESERVED".equals(event.status())) return;
        log.info("[NotifyService] Received confirmation for correlationId: {} - Sending email to {}",
                event.correlationId(), event.customerEmail());
    }
}
