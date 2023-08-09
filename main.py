from libs.extract_csv import ExtractCSV
from libs.insert_db_data import consumption_db, energy_db, outages_db


if __name__ == "__main__":
    # Generate CSVs
    extract = ExtractCSV("DDLR_01062023.xlsx")
    extract.generate_outages_csv()
    extract.generate_consumption_csv()
    extract.generate_energy_csv()

    # Write to DBs
    outages_db()
    energy_db()
    consumption_db()
