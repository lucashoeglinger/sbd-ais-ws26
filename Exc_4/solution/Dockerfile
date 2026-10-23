  # todo
FROM golang:1.27 AS build
WORKDIR /app
COPY . /app
RUN sh /app/scripts/build-application.sh

FROM alpine:latest AS run
WORKDIR /app
COPY --from=build /app/ordersystem /app/ordersystem
CMD "/app/ordersystem"