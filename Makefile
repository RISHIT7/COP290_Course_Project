SYMBOL := SBIN
num_years := 1
# DATE := 2023-01-16
# NEW_DATE := $(shell date -d "$(DATE) -$(num_years) year" +"%Y-%m-%d")
# OUTPUT := $(addprefix $(SYMBOL), _sol)

all:
	pip install -r requirements.txt
	python3 main.py $(SYMBOL) $(num_years)

clean:
	@rm -f *.csv *.txt *.pkl *.parquet *.feather *.h5 *.json *.avro *.orc *.png
