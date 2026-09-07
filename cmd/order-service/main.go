// Command order-service runs the order service as its own process.
package main

import "github.com/develogo/kafka-demo/internal/services"

func main() { services.Main("order-service", services.RunOrder) }
