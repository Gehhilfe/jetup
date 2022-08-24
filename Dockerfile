FROM golang:1.19 AS build

# Set the Current Working Directory inside the container
WORKDIR /build

# We want to populate the module cache based on the go.{mod,sum} files.
COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

# Unit tests
RUN CGO_ENABLED=0 go test -v

# Build the Go app
RUN go build -o ./out/jetup-cli cmd/main.go

# Start fresh from a smaller image
FROM gcr.io/distroless/base
 
LABEL maintainer="gehhilfe"
USER nonroot:nonroot

COPY --from=build /build/out/jetup-cli /app/jetup-cli

ENTRYPOINT ["/app/jetup-cli"]