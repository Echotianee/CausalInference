import datetime
import os

import numpy as np
import pandas as pd
import random
import multiprocessing


class Client:
    def __init__(self, client_id,
                 client_folder_path: str = "client_data"):
        self.client_id = client_id
        self.client_purchases_table_path = os.path.join(client_folder_path, self.client_id, "purchases.csv")
        self.client_events_table_path = os.path.join(client_folder_path, self.client_id, "events.csv")

        self.client_purchases_table = pd.read_csv(self.client_purchases_table_path, sep="|")
        self.client_events_table = pd.read_csv(self.client_events_table_path, sep="|")

    def process_client_add_purchase_nr_to_event_write_to_csv(self):
        """
        This function will process the client and add the purchases_7_day_after and purchases_30_day_after columns
        to the events.csv file
        :return:
        """
        self.client_events_table["purchases_7_day_after"] = 0
        self.client_events_table["purchases_30_day_after"] = 0

        for index, row in self.client_events_table.iterrows():
            user_id = row["USER_CLIENT_NUMBER"]
            date = row["DATE"]
            proposition_id = row["PROPOSITION"]
            # Replace these with your actual function to calculate events prior to purchase date
            purchases_30_day_after = self.get_purchases_after_date(self.client_purchases_table, user_id, date,
                                                                   proposition_id, date_range=30)
            purchases_7_day_after = self.get_purchases_after_date(self.client_purchases_table, user_id, date,
                                                                  proposition_id, date_range=7)

            self.client_events_table.loc[index, 'purchases_7_day_after'] = len(purchases_7_day_after)
            self.client_events_table.loc[index, 'purchases_30_day_after'] = len(purchases_30_day_after)

    def process_client_add_total_spend_on_product(self):
        """
        This function will process the client and add the total_spend_on_product column to the events.csv file
        Returns:

        """

        for index, row in self.client_events_table.iterrows():
            user_id = row["USER_CLIENT_NUMBER"]
            date = row["DATE"]
            proposition_id = row["PROPOSITION"]
            # Replace these with your actual function to calculate events prior to purchase date

            self.client_events_table.loc[index, 'total_spend_on_product'] = self.total_amount_spend_on_product(
                proposition_id, date, user_id)

    def total_amount_spend_on_product(self, proposition_id, date, user_id):
        """
        This function will calculate the total amount spend on the product. before this date for the client
        :param proposition_id:
        :param date: until this date the total amount spend on the product will be calculated
        :param user_id: the user id of the client
        :return: total amount spend on the product
        """
        total_spend = 0
        date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
        product_purchase_events = self.client_purchases_table[
            (self.client_purchases_table["USER_CLIENT_NUMBER"] == user_id) &
            (self.client_purchases_table["DATE"] <= str(date)) &
            (self.client_purchases_table["PROPOSITION"] == proposition_id)

            ]
        total_spend = product_purchase_events["AMOUNT"].sum()
        return total_spend

    def setup_product_category(self):
        """
        this functions sets up the product category for the puchases table of each proposition id
        Returns:

        """
        self.client_purchases_table["ARTICLE_CATEGORIE"] = 0
        for proposition_id in self.client_purchases_table["PROPOSITION"].unique():
            product_category = self.get_product_category(proposition_id)
            #print(product_category)
            #print(self.client_purchases_table[self.client_purchases_table["PROPOSITION"]==proposition_id].to_html())

            for index, row in self.client_purchases_table[self.client_purchases_table["PROPOSITION"]==proposition_id].iterrows():
                row["ARTICLE_CATEGORIE"] = product_category



    def get_product_category(self, proposition_id):
        """
        This function will get the product category of the proposition id
        :param proposition_id:
        :return: the product category of the proposition id
        """
        all_product_rows_df = self.client_events_table[self.client_events_table["PROPOSITION"] == proposition_id]
        try:

            article_category = all_product_rows_df["ARTICLE_CATEGORIE"].values[0]

        except IndexError:
            article_category = np.nan

        return article_category

    def process_client_add_total_spend_on_category_product(self):
        """
        This function will process the client and add the total_spend_on_category_product column to the events.csv file
        Returns:

        """

        for index, row in self.client_events_table.iterrows():
            user_id = row["USER_CLIENT_NUMBER"]
            date = row["DATE"]
            article_category = row["ARTICLE_CATEGORIE"]
            # Replace these with your actual function to calculate events prior to purchase date

            self.client_events_table.loc[
                index, 'total_spend_on_category_product'] = self.total_amount_spend_on_category_product(
                article_category, date)

    def total_amount_spend_on_category_product(self, article_category, date):
        """
        This function will calculate the total amount spend on the product. before this date for the client
        :param article_category: the category of the product
        :param date: until this date the total amount spend on the product will be calculated
        :param user_id: the user id of the client
        :return: total amount spend on the product
        :TODO: remove the product_id_finder and make the file already have the product category
        """
        total_spend = 0
        date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
        try:
            bought_product_id_in_category = self.client_purchases_table[
                (self.client_purchases_table["ARTICLE_CATEGORIE"] == article_category)
            ]["PROPOSITION"].unique()
            print("bought_product_id_in_category", bought_product_id_in_category)

        except KeyError:
            print("No product category found for client", self.client_id)
            return 0

        product_purchase_events = self.client_purchases_table[
            (self.client_purchases_table["DATE"] <= str(date)) &
            (self.client_purchases_table["PROPOSITION"].isin(bought_product_id_in_category))

            ]
        total_spend = product_purchase_events["AMOUNT"].sum()


        return total_spend

    def write_tables_to_csv(self):
        """
        This function will write the tables to the csv
        :return:
        """
        self.client_purchases_table.to_csv(self.client_purchases_table_path, sep="|", index=False)
        self.client_events_table.to_csv(self.client_events_table_path, sep="|", index=False)

    @staticmethod
    def get_purchases_after_date(purchase_events, user_id, purchase_date, proposition_id, date_range=30,
                                 testing=False):
        """
        it gets all the events from the specific date for one day before and after the given date
        :param purchase_events:
        :param user_id:
        :param purchase_date: 2024-11-18
        :param date_range: 30 number of dates to look after the event date
        :param testing: if True, it will print the number of events found
        :return: all events previous to the purchase date
        """
        date = datetime.datetime.strptime(purchase_date, "%Y-%m-%d").date()
        date_plus_date_range = date + datetime.timedelta(days=date_range)
        events = purchase_events[
            (purchase_events["USER_CLIENT_NUMBER"] == user_id) &
            (purchase_events["DATE"] <= str(date_plus_date_range)) &
            (purchase_events["DATE"] >= str(date)) &
            (purchase_events["PROPOSITION"] == proposition_id)

            ]
        if testing:
            if len(events) == 0:
                print("No events for the given date")

            else:
                print("Proposition ID: ", proposition_id)
                print("Random proposition ID: ", random.choice(events["PROPOSITION"].values))
                print("number of events found in last 30 days: ", len(events))

        return events

    def get_table_of_client(self, table_name):
        if table_name == "purchases":
            return self.client_purchases_table
        elif table_name == "events":
            return self.client_events_table
        else:
            print("Table name not recognized")
            return None


class ProcessClientsFolderTree:
    def __init__(self):
        self.client_path = "client_data"

        self.clientid_list = []
        self.process_clients_setup()

    def process_clients_setup(self):
        """
        This function will setup the clients in the client folder. It will add all of the clients to the clientid_list
        :return:
        """
        for client_id in os.listdir(self.client_path):
            if "." not in client_id:
                self.clientid_list.append(client_id)
            else:
                print("Skipping file: ", client_id)

    def process_clients_in_chunk_7days(self, client_id_chunk, add_purchases=True,
                                       add_total_product_spend=False,
                                       add_total_category_product_spend=False):
        """
        This function will process the clients in the chunk and add the purchases_7_day_after and purchases_30_day_after
        :param client_id_chunk:
        :param add_purchases: if True, it will add the purchases_7_day_after and purchases_30_day_after columns
        :param add_total_product_spend: if True, it will add the total_spend_on_product column
        :param add_total_category_product_spend: if True, it will add the total_spend_on_category_product column

        :return:
        """
        print("arguments:", client_id_chunk, add_purchases, add_total_product_spend, add_total_category_product_spend)
        for index, client_id in enumerate(client_id_chunk):
            if index % 10 == 0:
                print("Processing client: ", client_id)
            client = Client(client_id=client_id, client_folder_path="client_data")
            if add_purchases:
                client.process_client_add_purchase_nr_to_event_write_to_csv()

            if add_total_product_spend:
                client.process_client_add_total_spend_on_product()

            if add_total_category_product_spend:
                client.process_client_add_total_spend_on_category_product()



            client.write_tables_to_csv()
            #print("Done with client: ", client_id)

    def setup_chunks_from_client_list(self, client_list, chunk_size=100):
        """
        This function will setup the clients in chunk in size of chunk_size
        It return the chunks based on hte chunk size
        """
        chunks = [client_list[i:i + chunk_size] for i in range(0, len(client_list), chunk_size)]
        return chunks

    def process_client_purchases_to_event_multiprocessing(self, add_purchases=True,
                                                          add_total_product_spend=False,
                                                          add_total_category_product_spend=False):
        """
        This function will process the clients in the client folder and add the purchases_7_day_after
        and purchases_30_day_after columns to the events.csv file
        :param add_purchases: if True, it will add the purchases_7_day_after and purchases_30_day_after columns
        :param add_total_product_spend: if True, it will add the total_spend_on_product column
        :param add_total_category_product_spend: if True, it will add the total_spend_on_category_product column
        :return:
        """
        chunks_to_process = self.setup_chunks_from_client_list(self.clientid_list, chunk_size=100)

        args_for_processing = [
            (chunk,
             add_purchases,
             add_total_product_spend,
             add_total_category_product_spend
             )
            for chunk in chunks_to_process]

        with multiprocessing.Pool(processes=1) as pool:
            results = pool.starmap(self.process_clients_in_chunk_7days, args_for_processing)

        """if add_purchases:
            with multiprocessing.Pool(processes=1) as pool:
                results = pool.map(self.process_clients_in_chunk_7days, chunks_to_process)
        
        if add_total_product_spend:
            with multiprocessing.Pool(processes=1) as pool:
                results = pool.map(self.process_clients_in_chunk_7days, chunks_to_process)"""

    #________________________________Aggregate with multiprocessing_________________________________________________#

    def aggregate_clients_in_chunk(self, client_id_chunk, table_name="purchases"):
        """
        This function will process the clients in the chunk
        :param client_id_chunk:
        :param table_name: to aggregate can only be purchases or events
        :return:
        """
        if table_name == "purchases":
            client = Client(client_id=client_id_chunk[0], client_folder_path="client_data")
            df = client.get_table_of_client(table_name)
            for index, client_id in enumerate(client_id_chunk[1:]):
                client = Client(client_id=client_id, client_folder_path="client_data")
                df = pd.concat([df, client.get_table_of_client(table_name)], ignore_index=True)
        elif table_name == "events":
            client = Client(client_id=client_id_chunk[0], client_folder_path="client_data")
            df = client.get_table_of_client(table_name)
            for index, client_id in enumerate(client_id_chunk[1:]):
                client = Client(client_id=client_id, client_folder_path="client_data")
                df = pd.concat([df, client.get_table_of_client(table_name)], ignore_index=True)
        return df

    def aggregate_clients_multi_processing(self, table_name="purchases", write_to_csv=False,
                                           file_name_to_write="processed_purchase_events.csv", sample_bool=False):
        """
        This function will aggregate the clients in the client folder using multiprocessing
        You can specify the table name to aggregate
        :param table_name:
        :param write_to_csv:
        :param file_name_to_write:
        :param sample_bool: if True, it will only process the first 10 chunks which is 1000 clients
        :return:
        """
        chunks_to_process = self.setup_chunks_from_client_list(self.clientid_list, chunk_size=100)
        if sample_bool:
            chunks_to_process = chunks_to_process[:10]
        # Prepare tuples of arguments
        args_for_processing = [(chunk, table_name) for chunk in chunks_to_process]

        with multiprocessing.Pool(processes=8) as pool:
            results = pool.starmap(self.aggregate_clients_in_chunk, args_for_processing)

        df = pd.concat(results, ignore_index=True)
        if write_to_csv:
            df.to_csv(file_name_to_write, sep="|", index=False)
        return df


if __name__ == "__main__":
    #if you run this program, you will add each of the clients in the client_data folder. THen for each of the
    # clients you will add the purchases_7_day_after and purchases_30_day_after columns to the events.csv file
    if not os.path.exists("client_data/230/events.csv"):
        raise Exception(
            "you have to first setup the client tree structure with save_clients_in_folder_structure_updated.ipynb")
        exit()
    process_clients = ProcessClientsFolderTree()
    client_id_list = process_clients.clientid_list
    chunks_to_process = process_clients.setup_chunks_from_client_list(client_id_list, chunk_size=100)

    # If you want to process the clients in the client folder and add the purchases_7_day_after
    # and purchases_30_day_after columns to the events.csv file, you can use the function below

    process_clients.process_client_purchases_to_event_multiprocessing(
        add_purchases=False,
        add_total_product_spend=True,
        add_total_category_product_spend=True)



    # If you want to aggregate the clients in the client folder using multiprocessing, you can use the function below
    # This function will aggregate the clients in the client folder using multiprocessing

    """process_clients.aggregate_clients_multi_processing(table_name="purchases", write_to_csv=True,
                                                       file_name_to_write="processed_data/processed_purchase_events_final.csv")

    process_clients.aggregate_clients_multi_processing(table_name="events", write_to_csv=True,
                                                       file_name_to_write="processed_data/processed_events_final.csv")
"""