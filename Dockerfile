FROM golang:1.27.1-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
COPY internal/ ./internal/
RUN CGO_ENABLED=0 go build -trimpath -o /weather-app .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /weather-app ./weather-app
USER 10001:10001
ENV HTTP_ADDR=0.0.0.0:8080
EXPOSE 8080
CMD ["./weather-app"]
