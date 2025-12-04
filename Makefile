.PHONY: install lint check test pipeline step

install:
	poetry install --with dev

lint:
	poetry run ruff check .

check:
	poetry run python -m compileall -q src
	poetry run python -m compileall -q main.py

test:
	poetry run pytest -q

pipeline:
	poetry run python main.py

# Usage: make step STEP=load
step:
	poetry run python main.py --step $(STEP)

