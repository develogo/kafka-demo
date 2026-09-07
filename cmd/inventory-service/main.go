// Command inventory-service runs the inventory service as its own process.
package main

import "github.com/develogo/kafka-demo/internal/services"

func main() { services.Main("inventory-service", services.RunInventory) }
