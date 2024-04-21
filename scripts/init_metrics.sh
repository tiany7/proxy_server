#!/bin/bash
help() {
    echo "Usage: $0 [start|stop]"
    echo "  start: starting grafana/prometheus services"
    echo "  stop: cleaning up grafana/prometheus services"
}


start() {
    echo "Starting metrics services..."
    docker-compose up -d
}


stop() {
    echo "Cleaning up metrics services..."
    docker-compose down
}


case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    *)
        help
        exit 1
        ;;
esac

exit 0
