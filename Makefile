.PHONY: setup test backend-test frontend-test build api-types backend frontend

UV ?= $(shell command -v uv 2>/dev/null || printf '%s' "$$HOME/.local/bin/uv")

setup:
	cd backend && "$(UV)" sync --frozen
	bash scripts/frontend.sh install

test: backend-test frontend-test

backend-test:
	cd backend && env -i PATH="$$PATH" HOME="$$HOME" LANG=C.UTF-8 PYTHONDONTWRITEBYTECODE=1 FLASK_CONFIG=testing DOMEYE_CORE_SKIP_LOCAL_ENV=true DOMEYE_LOG_DIR="$(CURDIR)/.local/test-logs" .venv/bin/python -m pytest

frontend-test:
	bash scripts/frontend.sh test

build:
	bash scripts/frontend.sh build

api-types:
	bash scripts/frontend.sh api:types

backend:
	bash scripts/backend.sh

frontend:
	bash scripts/frontend.sh
