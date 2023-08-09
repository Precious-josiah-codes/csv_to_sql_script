import pandas as pd


class ExtractCSV:
    def __init__(self, excel_file_path):
        self.excel_file_path = excel_file_path
        self.xls = pd.ExcelFile(self.excel_file_path)
        self.sheet_names = self.xls.sheet_names
        self.state_sheets = self.sheet_names[2:]
        self.hourly_sheets = self.sheet_names[1]
        self.day_month_year = self.excel_file_path[-13:-5]
        self.day, self.month, self.year = (
            self.day_month_year[:2],
            self.day_month_year[2:4],
            self.day_month_year[-4:],
        )
        self.sheet_date = f"{self.day}-{self.month}-{self.year}"

    def __repr__(self):
        return self.day

    def generate_outages_csv(self):
        print("==========>>>> Started operations for outages.csv")
        state_outage_raw_df = []
        cleaned_state_outage_raw_df = []

        # getting the sheets for state outage
        print("==========>>>> Getting the sheets for state outages")
        outages_state_sheet = list(
            filter(lambda state: "Outages" in state, self.state_sheets)
        )

        # creating a dataframe list from the excel sheet
        print("==========>>>> Processing the dataframes")
        for sheet_name in outages_state_sheet:
            state_outage_raw_df.append(
                pd.read_excel(self.excel_file_path, sheet_name=sheet_name)
            )

        for state_consumption_df in state_outage_raw_df:
            # initializing a new outage dataframe
            outages_df = state_consumption_df

            # Extracting the head
            header = outages_df.iloc[1, :]

            # Extracting the body
            outage_df = outages_df.iloc[2:, :]

            # appending all outage data to the cleaned dataframe list
            cleaned_state_outage_raw_df.append(outage_df)

        # Joining all the dataframes together
        outage_df = pd.concat(cleaned_state_outage_raw_df).reset_index(drop=True)
        outage_df.columns = header
        print("==========>>>> Done processing the dataframes")

        print("==========>>>> Writing to outages.csv")
        outage_df.to_csv("outage.csv", index=False)
        print("==========>>>> outages.csv has been successfully written to your file.")

    def generate_consumption_csv(self):
        print("==========>>>> Started operations for consumption.csv")
        # declared variables
        consumption_dfs_list = []
        state_consumption_raw_dfs_list = []
        time_interval_dfs = []
        main_dfs = []

        # getting the sheets for state concumptions
        print("==========>>>> Getting the sheets for state consumption")
        consumption_state_sheet = list(
            filter(lambda state: "Outages" not in state, self.state_sheets)
        )

        # creating a dataframe list from the excel sheet
        print("==========>>>> Processing the dataframes")
        for sheet_name in consumption_state_sheet:
            state_consumption_raw_dfs_list.append(
                pd.read_excel(self.excel_file_path, sheet_name=sheet_name)
            )

        for state_consumption_raw_df in state_consumption_raw_dfs_list:
            # Set row 2 as the new header
            new_header = state_consumption_raw_df.iloc[2, :11]

            # SLice out row 3 leaving the rest.
            # NOTE: This is done to create a new dataframe to append the header
            state_df_body = state_consumption_raw_df.iloc[3:, :11]

            # Appending the header
            state_df_body.columns = new_header

            main_df = state_df_body

        # drop bottom rows not needed
        main_df.drop(main_df.index[-12:], inplace=True)

        # duplicating the rows selected for each timeinterval 24 times
        duplicated_rows = pd.concat([main_df.loc[:]] * 24)
        main_dfs.append(duplicated_rows)

        # concatenating all the time interval df into one and also resetting the index
        main_df = pd.concat(main_dfs).reset_index(drop=True)

        for state_consumption_raw_df in state_consumption_raw_dfs_list:
            # getting all the time columns
            time_row = state_consumption_raw_df.iloc[1:, 11:]

            # Number of columns in each time column
            columns_per_chunk = 9

            # Calculate the number of column chunks needed
            total_columns = len(time_row.columns)
            column_num_chunks = (
                total_columns + columns_per_chunk - 1
            ) // columns_per_chunk

            # List to store the time DataFrames and main dataframe
            each_time_df = []

            # Split the DataFrame into smaller chunks
            for i in range(column_num_chunks):
                start_col = i * columns_per_chunk
                end_col = (i + 1) * columns_per_chunk
                each_time_df.append(time_row.iloc[:, start_col:end_col])

        time_dfs = each_time_df

        # looping through the time interval dataframes
        for time_df in time_dfs:
            # drop bottom rows needed
            time_df.drop(time_df.index[-12:], inplace=True)

            # get the head
            header = time_df.iloc[1, :]
            # NOTE: FIX THIS ==>> REMOVE THE FIRST CELL FOR THE HEADER

            # get the time
            time = time_df.iloc[0, 0]

            # drop top row not needed
            time_df.drop(time_df.index[:2], inplace=True)

            # change the header
            time_df.columns = header

            # add the date column and the respective value
            time_df["DATE"] = [self.sheet_date] * len(time_df)

            # add the time column and the respective value
            time_df["TIME"] = [time] * len(time_df)

            # resetting the dataframe index
            reset_index = time_df.reset_index(drop=True)

            # save the time interval dataframe to a list
            time_interval_dfs.append(reset_index)

        # concatenating all the time interval df into one and also resetting the index
        time_interval_df = pd.concat(time_interval_dfs).reset_index(drop=True)
        consumption_df = pd.concat([main_df, time_interval_df], axis=1)
        print("==========>>>> Done processing the dataframes")

        print("==========>>>> Writing to consumption.csv")
        consumption_df.to_csv("consumption.csv", index=False)
        print(
            "==========>>>> consumption.csv has been successfully written to your file."
        )

    def generate_energy_csv(self):
        print("==========>>>> Started operations for energy.csv")
        df = pd.read_excel(self.excel_file_path, sheet_name=self.hourly_sheets)

        print("==========>>>> Processing the dataframes")
        # new_header
        main_df = df.iloc[2:, :]

        # Split the DataFrame into smaller chunks
        each_table_df = []

        for i in range(0, len(main_df), 14):
            df_cleaned = main_df.dropna(how="all")
            array_slice = df_cleaned.iloc[i : i + 13]
            array_slice.reset_index(drop=True)
            each_table_df.append(array_slice)

        # each_table_df = newf[1]
        all_time_df_list = []

        # slicing the disco name column from the time interval column
        disco_name_column_df = each_table_df[0].iloc[:, :1]

        # get header for the disco column
        disco_name_column_df.columns = ["DISCO"]
        disco_column_df = disco_name_column_df.iloc[2:, :]
        duplicated_disco_name_rows = pd.concat(
            [disco_column_df.loc[:]] * (len(each_table_df[:-1]) * 2)
        )
        disco_df = duplicated_disco_name_rows.reset_index(drop=True)

        # looping through the time interval dataframes
        for table_df in each_table_df[:-1]:
            # slicing the time intervals from the disco name column
            both_time_interval_df = table_df.iloc[:, 1:]

            # getting the column for the head
            both_time_interval_head_df = both_time_interval_df.iloc[1:2]

            # slice the first 11 header columns
            time_interval_head_df = both_time_interval_head_df.iloc[:, :11]
            time_interval_head_df = time_interval_head_df.iloc[0]

            # slice the first and second time interval
            first_time_df = both_time_interval_df.iloc[:, :11]
            second_time_df = both_time_interval_df.iloc[:, 11:]

            # get the time for each time dfs
            first_time = str(first_time_df.iloc[0, 0])
            second_time = str(second_time_df.iloc[0, 0])

            # further slice the first and second time interval
            first_time_df = first_time_df.iloc[2:, :]
            second_time_df = second_time_df.iloc[2:, :]

            # change the header
            first_time_df.columns = time_interval_head_df
            second_time_df.columns = time_interval_head_df

            # add the date column and the respective value
            first_time_df["DATE"] = [self.sheet_date] * len(first_time_df)
            second_time_df["DATE"] = [self.sheet_date] * len(second_time_df)

            # add the time column and the respective value
            first_time_df["TIME"] = [first_time] * len(first_time_df)
            second_time_df["TIME"] = [second_time] * len(second_time_df)

            # concatenating both the time intervals vertically
            time_interval_df = pd.concat(
                [first_time_df, second_time_df], axis=0
            ).reset_index(drop=True)

            # adding the result of dataframes to a list
            all_time_df_list.append(time_interval_df)

            # concatenating all time dataframes to one vertically
            time_df = pd.concat(all_time_df_list, axis=0).reset_index(drop=True)

            # combining both the disco and time dataframe together
            energy_df = pd.concat([disco_df, time_df], axis=1)
            print("==========>>>> Done processing the dataframes")

            print("==========>>>> Writing to energy.csv")
            energy_df.to_csv("energy.csv", index=False)
            print(
                "==========>>>> energy.csv has been successfully written to your file."
            )
