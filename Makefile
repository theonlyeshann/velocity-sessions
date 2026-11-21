.PHONY: build native fixtures-native fixtures-docker
build:
	docker buildx build --platform linux/amd64 --load -t velocity-sessions:latest .
native:
	g++ -O3 -std=c++20 -o velocity src/main.cpp
fixtures-native: native
	python3 tests/check_csv.py
fixtures-docker:
	python3 scripts/run_fixtures.py fixtures -- python3 tests/docker_fixture.py
