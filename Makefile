.PHONY: all clean run-broker run-producer run-consumer setup

# directories
DATA_DIR := ./data

setup:
	go run cmd/admin/main.go --create=test --num=5
	for i in 1 2 3 4 5; do \
		$(MAKE) run-producer; \
	done
clean:
	rm -rf $(DATA_DIR)

run-broker:
	go run main.go

run-producer:
	go run cmd/producer/main.go 

run-consumer:
	go run cmd/consumer/main.go 

# fresh start — clean data then start broker
fresh: clean run-broker
