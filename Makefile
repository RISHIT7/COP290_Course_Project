SYMBOL := SBIN
num_years := 1
# DATE := 2023-01-16
# NEW_DATE := $(shell date -d "$(DATE) -$(num_years) year" +"%Y-%m-%d")
# OUTPUT := $(addprefix $(SYMBOL), _sol)

all:
	python3 main.py $(SYMBOL) $(num_years)