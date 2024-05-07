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


    def process_clients_chunk(self):

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


class ProcessClientsFolderTree:
    def __init__(self):
        self.client_path = "client_data"

        self.clientid_list = []
        self.process_clients_setup()

    def process_clients_setup(self):
        for client_id in os.listdir(self.client_path):
            if "." not in client_id:
                self.clientid_list.append(client_id)
            else:
                print("Skipping file: ", client_id)


def process_clients_in_chunk(client_id_chunk):
    """
    This function will process the clients in the chunk
    :param client_id_chunk:
    :return:
    """
    for index, client_id in enumerate(client_id_chunk):
        if index % 10 == 0:
            print("Processing client: ", client_id)
        client = Client(client_id=client_id, client_folder_path= "client_data")
        client.process_clients_chunk()
        print("Done with client: ", client_id)

def setup_chunks_from_client_list(client_list, chunk_size=100):
    """
    This function will setup the clients in chunk in size of chunk_size
    It return the chunks based on hte chunk size
    """
    chunks = [client_list[i:i + chunk_size] for i in range(0, len(client_list), chunk_size)]
    return chunks



if __name__ == "__main__":
    process_clients = ProcessClientsFolderTree()
    client_id_list = process_clients.clientid_list
    chunks_to_process = setup_chunks_from_client_list(client_id_list, chunk_size=100)

    with multiprocessing.Pool(processes=8) as pool:

        results = pool.map(process_clients_in_chunk, chunks_to_process)

