
transference: transference.py
	mypy transference.py
	pytest-3 transference.py
	python3 ./strip.py
