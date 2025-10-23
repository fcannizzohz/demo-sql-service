# Use docker compose by default; override with `make COMPOSE=docker-compose ...` if needed
COMPOSE ?= docker compose
VOLUME ?= docker volume

.DEFAULT_GOAL := up
.PHONY: up down reset

up:
	@$(COMPOSE) up -d

up-force:
	@$(COMPOSE) up -d --force-recreate --build

down:
	@$(COMPOSE) down

reset:
	@$(COMPOSE) down -v
	@rm -f ./state/promtail-positions.yaml
	@${VOLUME} prune -f
