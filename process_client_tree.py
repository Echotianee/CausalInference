import datetime
import os
import pandas as pd
import random
import multiprocessing


class Client:
    def __init__(self, client_id, client_folder_path):
        self.client_id = client_id
        self.client_purchases_table_path = os.path.join(client_folder_path, self.client_id, "purchases.csv")
        self.client_events_table_path = os.path.join(client_folder_path, self.client_id, "events.csv")

        self.client_purchases_table = pd.read_csv(self.client_purchases_table_path, sep="|")
        self.client_events_table = pd.read_csv(self.client_events_table_path, sep="|")
        self.client_events_table["purchases_7_day_after"] = 0
        self.client_events_table["purchases_30_day_after"] = 0


    def process_client_add_purchase_nr_to_event_write_to_csv(self):
        """
        This function will process the client and add the purchases_7_day_after and purchases_30_day_after columns
        to the events.csv file
        :return:
        """

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
        self.client_events_table.to_csv(self.client_events_table_path, sep="|", index=False)

    @staticmethod
    def get_purchases_after_date(purchase_events, user_id, purchase_date, proposition_id, date_range=30,
                                 testing=False):
        """
        it gets all the events from the specific date for one day before and after the given date
        :param data_events:
        :param user_id:
        :param date: 2024-11-18
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


    def process_clients_in_chunk(self, client_id_chunk):
        """
        This function will process the clients in the chunk
        :param client_id_chunk:
        :return:
        """
        for index, client_id in enumerate(client_id_chunk):
            if index % 10 == 0:
                print("Processing client: ", client_id)
            client = Client(client_id=client_id, client_folder_path= "client_data")
            client.process_client_add_purchase_nr_to_event_write_to_csv()
            print("Done with client: ", client_id)

    def setup_chunks_from_client_list(self, client_list, chunk_size=100):
        """
        This function will setup the clients in chunk in size of chunk_size
        It return the chunks based on hte chunk size
        """
        chunks = [client_list[i:i + chunk_size] for i in range(0, len(client_list), chunk_size)]
        return chunks

    def process_client_purchases_to_event_multiprocessing(self):
        """
        This function will process the clients in the client folder and add the purchases_7_day_after
        and purchases_30_day_after columns to the events.csv file
        :return:
        """
        chunks_to_process = self.setup_chunks_from_client_list(self.clientid_list, chunk_size=100)

        with multiprocessing.Pool(processes=8) as pool:

            results = pool.map(self.process_clients_in_chunk, chunks_to_process)

    #________________________________Aggregate with multiprocessing_________________________________________________#

    def aggregate_clients_in_chunk(self, client_id_chunk, table_name="purchases"):
        """
        This function will process the clients in the chunk
        :param client_id_chunk:
        :param table_name: to aggregate can only be purchases or events
        :return:
        """
        if table_name == "purchases":
            client = Client(client_id=client_id_chunk[0], client_folder_path= "client_data")
            df = client.get_table_of_client(table_name)
            for index, client_id in enumerate(client_id_chunk[1:]):
                client = Client(client_id=client_id, client_folder_path= "client_data")
                df = pd.concat([df, client.get_table_of_client(table_name)], ignore_index=True)
        elif table_name == "events":
            client = Client(client_id=client_id_chunk[0], client_folder_path= "client_data")
            df = client.get_table_of_client(table_name)
            for index, client_id in enumerate(client_id_chunk[1:]):
                client = Client(client_id=client_id, client_folder_path= "client_data")
                df = pd.concat([df, client.get_table_of_client(table_name)], ignore_index=True)
        return df

    def aggregate_clients_multi_processing(self, table_name="purchases", write_to_csv=False,
                                           file_name_to_write="processed_purchase_events.csv"):
        """
        This function will aggregate the clients in the client folder using multiprocessing
        You can specify the table name to aggregate
        :param table_name:
        :param write_to_csv:
        :param file_name_to_write:
        :return:
        """
        chunks_to_process = self.setup_chunks_from_client_list(self.clientid_list, chunk_size=100)

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
    process_clients = ProcessClientsFolderTree()
    client_id_list = process_clients.clientid_list
    chunks_to_process = process_clients.setup_chunks_from_client_list(client_id_list, chunk_size=100)
    process_clients.process_client_purchases_to_event_multiprocessing()

    #with multiprocessing.Pool(processes=8) as pool:

    #    results = pool.map(process_clients.process_clients_in_chunk, chunks_to_process)

    #df_list = []
    #with multiprocessing.Pool(processes=8) as pool:

    #    results = pool.map(process_clients.aggregate_clients_in_chunk, chunks_to_process)
    #process_clients.aggregate_clients_multi_processing(table_name="purchases", write_to_csv=True,
    #                                                   file_name_to_write="processed_purchase_events.csv")

    #process_clients.aggregate_clients_multi_processing(table_name="events", write_to_csv=True,
    #                                                   file_name_to_write="processed_events.csv")

