import csv
from datetime import datetime
from sqlalchemy import Numeric, Time, create_engine, Column, Integer, String, Date

from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base


# engine = create_engine("sqlite:///mydb.db", echo=True)


def energy_db():
    print("===>>> Writing to energy table")
    DATABASE_URL = "postgresql://postgres:preciousdB@localhost:5432/postgres"
    # engine = create_engine("sqlite:///mydb.db", echo=True)
    engine = create_engine(DATABASE_URL)
    Base = declarative_base()
    session = sessionmaker(bind=engine)

    session = session()

    class Energy(Base):
        __tablename__ = "energy"
        id = Column(Integer, primary_key=True)
        disco = Column(String)
        contract_load = Column(Numeric(precision=10, scale=3))
        total_forecast = Column(Numeric(precision=10, scale=3))
        load_allocation = Column(Numeric(precision=10, scale=3))
        total_energy_consumed = Column(Numeric(precision=10, scale=3))
        contract_forecast_variation = Column(Numeric(precision=10, scale=3))
        contract_allocation_variation = Column(Numeric(precision=10, scale=3))
        disco_contract_load_variation = Column(Numeric(precision=10, scale=3))
        disco_allocation_load_variation = Column(Numeric(precision=10, scale=3))
        disco_liability = Column(Numeric(precision=10, scale=3))
        tcn_liability = Column(Numeric(precision=10, scale=3))
        genco_liability = Column(Numeric(precision=10, scale=3))
        date = Column(Date)
        time = Column(Time)

    Base.metadata.create_all(engine)

    with open("energy.csv", "r") as csv_file:
        csv_reader = csv.DictReader(csv_file)

        for row in csv_reader:
            disco = row["DISCO"]
            contract_load = row["CONTRACT LOAD"]
            total_forecast = row["TOTAL FORECAST"]
            load_allocation = row["LOAD ALLOCATION"]
            total_energy_consumed = row["TOTAL ENERGY CONSUMED"]
            contract_forecast_variation = row["CONTRACT FORECAST VARIATION"]
            contract_allocation_variation = row["CONTRACT ALLOCATION VARIATION"]
            disco_contract_load_variation = row["DISCO CONTRACT LOAD VARIATION"]
            disco_allocation_load_variation = row["DISCO ALLOCATION LOAD VARIATION"]
            disco_liability = row["DISCO LIABILITY"]
            tcn_liability = row["TCN LIABILITY"]
            genco_liability = row["GENCO LIABILITY"]
            date = (
                datetime.strptime(row["DATE"], r"%d-%m-%Y").date()
                if row["DATE"]
                else None
            )
            time = (
                datetime.strptime(row["TIME"], r"%H:%M:%S").time()
                if len(row["TIME"]) > 5
                else datetime.strptime(row["TIME"], r"%M:%S").time()
            )

            new_record = Energy(
                disco=disco,
                contract_load=contract_load,
                total_forecast=total_forecast,
                load_allocation=load_allocation,
                total_energy_consumed=total_energy_consumed,
                contract_forecast_variation=contract_forecast_variation,
                contract_allocation_variation=contract_allocation_variation,
                disco_contract_load_variation=disco_contract_load_variation,
                disco_allocation_load_variation=disco_allocation_load_variation,
                disco_liability=disco_liability,
                tcn_liability=tcn_liability,
                genco_liability=genco_liability,
                date=date,
                time=time,
            )
            session.add(new_record)

        # Commit the changes to the database
        session.commit()

        # Close the session
        session.close()

    print("===>>> Data has been saved to energy table")
    print(" ")


def outages_db():
    print("===>>> Writing to outages table")
    DATABASE_URL = "postgresql://postgres:preciousdB@localhost:5432/postgres"
    engine = create_engine(DATABASE_URL)
    # engine = create_engine("sqlite:///mydb.db", echo=True)
    Base = declarative_base()
    session = sessionmaker(bind=engine)

    session = session()

    class Outage(Base):
        __tablename__ = "outage"
        id = Column(Integer, primary_key=True)
        disco = Column(String)
        region = Column(String)
        subregion_acc = Column(String)
        substation = Column(String)
        thirty_three_kv_feeder = Column(String)
        date_off = Column(Date)
        hour_off = Column(Time)
        minute_off = Column(Time)
        date_on = Column(Date)
        hour_On = Column(Time)
        minute_on = Column(Time)
        duration_of_outage = Column(Time)
        classs = Column(String)
        last_load_recorded = Column(Numeric(precision=10, scale=3))
        event_indication = Column(String)
        party_responsible = Column(String)
        name_designation_of_officer_confirming_interruption = Column(String)
        name_designation_of_officer_confirming_restoration = Column(String)
        weather_condition = Column(String)
        remarks = Column(String)

    Base.metadata.create_all(engine)

    with open("outage.csv", "r") as csv_file:
        csv_reader = csv.DictReader(csv_file)

        for row in csv_reader:
            disco = row["Disco"]
            region = row["Region"]
            subregion_acc = row["SubRegion/ACC"]
            substation = row["Substation"]
            thirty_three_kv_feeder = row["33kV Feeder"]
            date_off = (
                datetime.strptime(row["Date off"], r"%Y-%m-%d %H:%M:%S").date()
                if row["Date off"]
                else None
            )
            hour_off = (
                datetime.strptime(row["Hour Off"], r"%H:%M:%S").time()
                if row["Hour Off"]
                else None
            )
            minute_off = (
                datetime.strptime(row["Minute off"], r"%H:%M:%S").time()
                if row["Minute off"]
                else None
            )
            date_on = (
                datetime.strptime(row["Date on"], r"%Y-%m-%d %H:%M:%S").date()
                if row["Date on"]
                else None
            )
            hour_On = (
                datetime.strptime(row["Hour On"], r"%H:%M:%S").time()
                if row["Hour On"]
                else None
            )
            minute_on = (
                datetime.strptime(row["Minute on"], r"%H:%M:%S").time()
                if row["Minute on"]
                else None
            )
            duration_of_outage = (
                datetime.strptime(
                    row["Duration of Outage (H:mm)"].split(" ")[-1], r"%H:%M:%S"
                ).time()
                if row["Duration of Outage (H:mm)"]
                else None
            )
            classs = row["Class"]
            last_load_recorded = row["Last Load Recorded (MW)"]
            event_indication = row["Event/Indication"]
            party_responsible = row["Party Responsible"]
            name_designation_of_officer_confirming_interruption = row[
                "Name/Designation of Officer Confirming  Interruption (DISCO)"
            ]
            name_designation_of_officer_confirming_restoration = row[
                "Name/Designation of Officer Confirming  Restoration (DISCO)"
            ]
            weather_condition = row["Weather Condition"]
            remarks = row["Remarks"]

            new_record = Outage(
                disco=disco,
                region=region,
                subregion_acc=subregion_acc,
                substation=substation,
                thirty_three_kv_feeder=thirty_three_kv_feeder,
                date_off=date_off,
                hour_off=hour_off,
                minute_off=minute_off,
                date_on=date_on,
                hour_On=hour_On,
                minute_on=minute_on,
                duration_of_outage=duration_of_outage,
                classs=classs,
                last_load_recorded=last_load_recorded,
                event_indication=event_indication,
                party_responsible=party_responsible,
                name_designation_of_officer_confirming_interruption=name_designation_of_officer_confirming_interruption,
                name_designation_of_officer_confirming_restoration=name_designation_of_officer_confirming_restoration,
                weather_condition=weather_condition,
                remarks=remarks,
            )
            session.add(new_record)

        # Commit the changes to the database
        session.commit()

        # Close the session
        session.close()
    print("===>>> Data has been saved to outages table")
    print(" ")


def consumption_db():
    print("===>>> Writing to consumption table")
    DATABASE_URL = "postgresql://postgres:preciousdB@localhost:5432/postgres"
    engine = create_engine(DATABASE_URL)
    Base = declarative_base()
    session = sessionmaker(bind=engine)

    session = session()

    class Consumption(Base):
        __tablename__ = "consumption"
        id = Column(Integer, primary_key=True)
        region = Column(String)
        disco = Column(String)
        area_control = Column(String)
        station = Column(String)
        transformer_name = Column(String)
        rating = Column(String)
        feeder_band = Column(String)
        associated_33kv_feeder = Column(String)
        tcn_limits = Column(Numeric)
        disco_base_load = Column(String)
        disco_peak_load = Column(String)
        forecast = Column(Numeric)
        actual_mw = Column(Numeric)
        difference = Column(Numeric)
        dur_of_disco_outage = Column(Time)
        dur_of_tcn_outage = Column(Time)
        dur_of_genco_outage = Column(Time)
        disco_liability = Column(Numeric)
        tcn_liability = Column(Numeric)
        genco_liability = Column(Numeric)
        date = Column(Date)
        time = Column(String)

    Base.metadata.create_all(engine)

    with open("consumption.csv", "r") as csv_file:
        csv_reader = csv.DictReader(csv_file)

        for row in csv_reader:
            region = row["REGION"]
            disco = row["DISCO"]
            area_control = row["AREA CONTROL"]
            station = row["STATION"]
            transformer_name = row["TRANSFORMER NAME"]
            rating = row["RATING"]
            feeder_band = row["FEEDER BAND"]
            associated_33kv_feeder = row["ASSOCIATED 33KV FEEDER"]
            tcn_limits = row["TCN Limits"] if len(str(row["TCN Limits"])) > 2 else 0.0
            disco_base_load = row["Disco Base Load"]
            disco_peak_load = row["Disco Peak Load"]
            forecast = row["FORECAST"] if len(str(row["FORECAST"])) > 2 else 0.0
            actual_mw = row["ACTUAL MW"] if len(str(row["ACTUAL MW"])) > 2 else 0.0
            difference = row["DIFFERENCE"] if len(str(row["DIFFERENCE"])) > 2 else 0.0
            dur_of_disco_outage = datetime.strptime(
                row["DUR OF DISCO OUTAGE"], r"%H:%M:%S"
            ).time()
            dur_of_tcn_outage = datetime.strptime(
                row["DUR OF TCN OUTAGE"], r"%H:%M:%S"
            ).time()
            dur_of_genco_outage = datetime.strptime(
                row["DUR OF GENCO OUTAGE"], r"%H:%M:%S"
            ).time()
            disco_liability = (
                row["DISCO LIABILITY"] if len(str(row["DISCO LIABILITY"])) > 2 else 0.0
            )
            tcn_liability = (
                row["TCN LIABILITY"] if len(str(row["TCN LIABILITY"])) > 2 else 0.0
            )
            genco_liability = (
                row["GENCO LIABILITY"] if len(str(row["GENCO LIABILITY"])) > 2 else 0.0
            )
            date = (
                datetime.strptime(row["DATE"], r"%d-%m-%Y").date()
                if row["DATE"]
                else None
            )
            time = row["TIME"]

            new_record = Consumption(
                region=region,
                disco=disco,
                area_control=area_control,
                station=station,
                transformer_name=transformer_name,
                rating=rating,
                feeder_band=feeder_band,
                associated_33kv_feeder=associated_33kv_feeder,
                tcn_limits=tcn_limits,
                disco_base_load=disco_base_load,
                disco_peak_load=disco_peak_load,
                forecast=forecast,
                actual_mw=actual_mw,
                difference=difference,
                dur_of_disco_outage=dur_of_disco_outage,
                dur_of_tcn_outage=dur_of_tcn_outage,
                dur_of_genco_outage=dur_of_genco_outage,
                disco_liability=disco_liability,
                tcn_liability=tcn_liability,
                genco_liability=genco_liability,
                date=date,
                time=time,
            )
            session.add(new_record)

        # Commit the changes to the database
        session.commit()

        # Close the session
        session.close()
    print("===>>> Data has been saved to consumption table")
    print(" ")
