FROM golang:1.22-alpine AS build
WORKDIR /app
ENV GOTOOLCHAIN=auto
COPY go.mod .
COPY *.go .
RUN go mod tidy && go build -o /controlplane .

FROM alpine:3.20
COPY --from=build /controlplane /controlplane
EXPOSE 8000
CMD ["/controlplane"]
