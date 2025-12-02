.PHONY: install pull-queries

install:
	@echo "Ensuring required tools are installed..."
	# Install pre-commit if missing and install the git hook
	@if ! command -v pre-commit >/dev/null 2>&1; then \
		python3 -m pip install --upgrade pip >/dev/null; \
		python3 -m pip install pre-commit; \
	fi
	@pre-commit --version >/dev/null 2>&1 || pre-commit install || true

	# Install pipx if missing (Homebrew on macOS)
	@if ! command -v pipx >/dev/null 2>&1; then \
		if command -v brew >/dev/null 2>&1; then \
			brew install pipx; \
			pipx ensurepath || true; \
		else \
			echo "brew not found; please install pipx manually"; \
		fi; \
	fi

	# Install poetry if missing; prefer pipx when available
	@if ! command -v poetry >/dev/null 2>&1; then \
		if command -v pipx >/dev/null 2>&1; then \
			pipx install poetry || pipx upgrade poetry || true; \
		else \
			python3 -m pip install --user poetry || true; \
		fi; \
	fi

	# Install pyenv if missing (Homebrew on macOS)
	@if ! command -v pyenv >/dev/null 2>&1; then \
		if command -v brew >/dev/null 2>&1; then \
			brew install pyenv; \
		else \
			echo "brew not found; please install pyenv manually if you need multiple Python versions"; \
		fi; \
	fi

	# Ensure pyenv has Python 3.11 if pyenv is available
	@pyenv install -s 3.11 || true; \

	# Finally install project dependencies via poetry
	@echo "Installing project dependencies with poetry..."
	@poetry install || (echo "poetry install failed; ensure poetry is on PATH" && false)

init-samples:
	@if [ ! -f .env ]; then \
  		echo ".env file not found! Please create one based on .env.example"; \
  		exit 1; \
	fi
	poetry run init_sample_data

pull-queries:
	@if [ ! -f .env ]; then \
  		echo ".env file not found! Please create one based on .env.example"; \
  		exit 1; \
	fi
	poetry run create_table_structure
	poetry run add_usage_data
