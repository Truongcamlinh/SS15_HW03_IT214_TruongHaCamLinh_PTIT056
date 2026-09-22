package com.storex.events;

public record SeatEvent(
        String correlationId,
        String bookingId,
        String customerEmail,
        String seatNumber,
        String paymentId,
        String status,
        String reason) {}
