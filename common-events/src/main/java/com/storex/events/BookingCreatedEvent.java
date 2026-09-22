package com.storex.events;

import java.math.BigDecimal;

public record BookingCreatedEvent(
        String correlationId,
        String bookingId,
        String customerEmail,
        String seatNumber,
        BigDecimal amount) {}
