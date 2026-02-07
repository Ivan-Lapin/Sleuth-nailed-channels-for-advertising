from src.utils.dates import parse_date_range

def main():
    dates_list = [
        "2026-02-02 - 2026-02-06",
        "2026-02-02-2026-02-06",
        "2026-02-02 - 2026-02-02"
    ]
    
    for date in dates_list:
        print(date)
        result = parse_date_range(date)
        print(f"'{date}' → {result}")
        
if __name__ == "__main__":
    main()