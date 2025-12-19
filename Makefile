.PHONY: all help clean extract-all consolidate \
	extract-policybazaar extract-icici extract-paisabazaar \
	extract-rc-transfer extract-shriramgi extract-fees \
	extract-permit extract-rto extract-carandbike-cars \
	extract-carandbike-cars-test clean-temp clean-all

# Default target
help:
	@echo "Available commands:"
	@echo "  make all                      - Extract all data and consolidate"
	@echo "  make extract-all              - Extract data from all sources"
	@echo "  make consolidate              - Consolidate all extracted FAQs"
	@echo ""
	@echo "Car data extraction:"
	@echo "  make extract-carandbike-cars      - Extract all CarAndBike cars (262 cars, ~5-10 min)"
	@echo "  make extract-carandbike-cars-test - Test extraction with 5 cars"
	@echo ""
	@echo "Individual FAQ extraction commands:"
	@echo "  make extract-policybazaar     - Extract PolicyBazaar FAQs"
	@echo "  make extract-icici            - Extract ICICI FAQs"
	@echo "  make extract-paisabazaar      - Extract PaisaBazaar FAQs"
	@echo "  make extract-rc-transfer      - Extract RC Transfer Q&A"
	@echo "  make extract-shriramgi        - Extract Shriram GI FAQs"
	@echo "  make extract-fees             - Extract RTO fees and charges"
	@echo "  make extract-permit           - Extract permit details"
	@echo "  make extract-rto              - Extract RTO FAQs"
	@echo ""
	@echo "Cleanup commands:"
	@echo "  make clean                    - Remove all generated data files"
	@echo "  make clean-temp               - Remove temporary files (.temp/)"
	@echo "  make clean-all                - Remove all generated and temp files"

# Extract all data and consolidate
all: extract-all consolidate

# Extract data from all sources
extract-all: extract-policybazaar extract-icici extract-paisabazaar \
	extract-rc-transfer extract-shriramgi extract-fees \
	extract-permit extract-rto

# Individual extraction targets
extract-policybazaar:
	@echo "Extracting PolicyBazaar FAQs..."
	conda run -n scrape python scripts/extract_policybazaar_faqs.py

extract-icici:
	@echo "Extracting ICICI FAQs..."
	conda run -n scrape python scripts/extract_icici_faqs.py

extract-paisabazaar:
	@echo "Extracting PaisaBazaar FAQs..."
	conda run -n scrape python scripts/extract_paisabazaar_faqs.py

extract-rc-transfer:
	@echo "Extracting RC Transfer Q&A..."
	conda run -n scrape python scripts/extract_rc_transfer_qa.py

extract-shriramgi:
	@echo "Extracting Shriram GI FAQs..."
	conda run -n scrape python scripts/extract_shriramgi_faqs.py

extract-fees:
	@echo "Extracting RTO fees and charges..."
	conda run -n scrape python scripts/extract_fees_charges.py

extract-permit:
	@echo "Extracting permit details..."
	conda run -n scrape python scripts/extract_permit_details.py

extract-rto:
	@echo "Extracting RTO FAQs..."
	conda run -n scrape python scripts/extract_rto_faq.py

# CarAndBike car data extraction
extract-carandbike-cars:
	@echo "Extracting CarAndBike car data (262 cars)..."
	@echo "This may take 5-10 minutes. The extraction is resumable."
	conda run -n scrape python scripts/extract_carandbike_new_car_data.py

extract-carandbike-cars-test:
	@echo "Testing CarAndBike extraction with 5 cars..."
	conda run -n scrape python scripts/extract_carandbike_new_car_data.py --limit 5

# CarAndBike bike data extraction
extract-carandbike-bikes:
	@echo "Extracting CarAndBike bike data..."
	conda run -n scrape python scripts/extract_carandbike_new_bike_data.py

# Consolidate all FAQs
consolidate:
	@echo "Consolidating all FAQs..."
	conda run -n scrape python scripts/consolidate_faqs.py

# Clean generated files
clean:
	@echo "Cleaning generated data files..."
	rm -f data/*.json data/*.csv
	rm -rf data/new_car_details
	@echo "Done!"

# Clean temp files
clean-temp:
	@echo "Cleaning temporary files..."
	rm -rf .temp
	@echo "Done!"

# Clean everything (data + temp)
clean-all: clean clean-temp
	@echo "All generated files cleaned!"
