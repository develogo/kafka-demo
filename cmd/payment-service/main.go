// Command payment-service runs the payment service as its own process.
package main

import "github.com/develogo/kafka-demo/internal/services"

func main() { services.Main("payment-service", services.RunPayment) }
