// Command projection-service maintains the view of the whole order lifecycle
// the front end reads. See docs/adr/0003.
package main

import "github.com/develogo/kafka-demo/internal/services"

func main() { services.Main("projection-service", services.RunProjection) }
