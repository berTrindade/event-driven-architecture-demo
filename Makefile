.PHONY: up down logs demo

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f

demo:
	./scripts/demo.sh
