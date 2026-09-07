// Command shipping-service runs the shipping service as its own process.
package main

import "github.com/develogo/kafka-demo/internal/services"

func main() { services.Main("shipping-service", services.RunShipping) }
