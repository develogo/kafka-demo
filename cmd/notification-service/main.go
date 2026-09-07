// Command notification-service runs the notification service as its own process.
package main

import "github.com/develogo/kafka-demo/internal/services"

func main() { services.Main("notification-service", services.RunNotification) }
