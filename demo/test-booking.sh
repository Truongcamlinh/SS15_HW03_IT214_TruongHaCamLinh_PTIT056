#!/usr/bin/env bash
set -euo pipefail

curl -sS -X POST http://localhost:8080/api/bookings \
  -H 'Content-Type: application/json' \
  -d '{"customerEmail":"camlinh@example.com","seatNumber":"A12","amount":1200000}'
printf '\n'

# Ghế bắt đầu bằng X được dùng để mô phỏng giữ ghế thất bại và hoàn tiền.
curl -sS -X POST http://localhost:8080/api/bookings \
  -H 'Content-Type: application/json' \
  -d '{"customerEmail":"camlinh@example.com","seatNumber":"X99","amount":1200000}'
printf '\n'
