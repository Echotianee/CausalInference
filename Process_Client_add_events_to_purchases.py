import pandas as pd
import datetime
import random
class Client:
    def __init__(self, client_id, client_purchases_table, client_events_table):
        self.client_id = client_id
        self.client_purchases_table = client_purchases_table
        self.client_events_table = client_events_table
        self.processed_purchase_events = pd.DataFrame(
            columns=["USER_CLIENT_NUMBER", "DATE", "PROPOSITION", "AMOUNT", "NUMBER_OF_TIMES_SEEN_30_days",
                     "NUMBER_OF_TIMES_SEEN_7_days", "NUMBER_OF_TIMES_SEEN_1_days"])

    def process_clients_chunk(self):

        for index, row in self.client_purchases_table.iterrows():
            user_id = row["USER_CLIENT_NUMBER"]
            date = row["DATE"]
            proposition_id = row["PROPOSITION"]
            amount = row["AMOUNT"]
            # Replace these with your actual function to calculate events prior to purchase date
            events_30_day_prior = self.get_events_previous_on_purchase_date(self.client_events_table, user_id, date,
                                                                       proposition_id, date_range=-30)
            events_7_day_prior = self.get_events_previous_on_purchase_date(self.client_events_table, user_id, date,
                                                                      proposition_id, date_range=-7)
            events_1_day_prior = self.get_events_previous_on_purchase_date(self.client_events_table, user_id, date,
                                                                      proposition_id, date_range=-1)

            self.processed_purchase_events.loc[len(self.processed_purchase_events.index)] = [
                user_id,
                date,
                proposition_id,
                amount,
                len(events_30_day_prior),
                len(events_7_day_prior),
                len(events_1_day_prior)
            ]

    @staticmethod
    def get_events_previous_on_purchase_date(data_events, user_id, purchase_date, proposition_id, date_range=-30,
                                             testing=False):
        """
        it gets all the events from the specific date for one day before and after the given date
        :param data_events:
        :param user_id:
        :param date: 2024-11-18
        :return: all events previous to the purchase date
        """
        date = datetime.datetime.strptime(purchase_date, "%Y-%m-%d").date()
        date_minus_five = date + datetime.timedelta(days=date_range)
        events = data_events[
            (data_events["USER_CLIENT_NUMBER"] == user_id) &
            (data_events["DATE"] >= str(date_minus_five)) &
            (data_events["DATE"] <= str(date)) &
            (data_events["PROPOSITION"] == proposition_id)

            ]
        if testing:
            if len(events) == 0:
                print("No events for the given date")

            else:
                print("Proposition ID: ", proposition_id)
                print("Random proposition ID: ", random.choice(events["PROPOSITION"].values))
                print("number of events found in last 30 days: ", len(events))

        return events


class Process_clients:
    def __init__(self, purchase_data, events_data):
        self.purchase_events_table = purchase_data
        self.events_table = events_data
        self.unique_clients = self.purchase_events_table["USER_CLIENT_NUMBER"].unique()
        self.clients = []
        self.process_clients_setup()
        self.chunks = self.setup_chunks()

    def process_clients_setup(self):
        for index, client_id in enumerate(self.unique_clients):
            if index % 10 == 0:
                print("Setting up client number: ", index)
            data_events_related_to_client = self.events_table[
                self.events_table["USER_CLIENT_NUMBER"] == client_id]

            purchase_events_related_to_client = self.purchase_events_table[
                self.purchase_events_table["USER_CLIENT_NUMBER"] == client_id]


            client = Client(client_id=client_id,
                            client_purchases_table=purchase_events_related_to_client,
                            client_events_table=data_events_related_to_client)

            self.clients.append(client)

    def setup_chunks(self, chunk_size=100):
        chunks = [self.clients[i:i + chunk_size] for i in range(0, len(self.clients), chunk_size)]
        return chunks

    def process_clients_in_chunk(self, chunk):
        """
        This function will process the clients in the chunk
        :param chunk:
        :return:
        """
        for index, client in enumerate(chunk):
            if index % 2 == 0:
                print("Processing client number: ", index)
            client.process_clients_chunk()

    def process_client_chunks(self):
        """
        This function will process the clients in the chunk using threading
        :param chunk:
        :return:
        """
        for chunk in self.chunks:
            self.process_clients_in_chunk(chunk)

    def aggregate_client_tables(self, file_name="processed_purchase_events.csv"):
        """
        This function will aggregate all the processed purchase events from all the clients
        :return:
        """
        processed_payment_table_combined = pd.DataFrame(columns=["USER_CLIENT_NUMBER", "DATE", "PROPOSITION",
                                                                 "AMOUNT",
                                                        "NUMBER_OF_TIMES_SEEN_30_days",
                                                        "NUMBER_OF_TIMES_SEEN_7_days", "NUMBER_OF_TIMES_SEEN_1_days"])
        for index, chunk in enumerate(self.chunks):
            print("Aggregating chunk number: ", index)
            for client in chunk:
                processed_payment_table_combined = pd.concat([
                    processed_payment_table_combined,
                    client.processed_purchase_events
                ])

        processed_payment_table_combined.to_csv(file_name, index=False, sep="|")

        return processed_payment_table_combined




if __name__ == "__main__":
    data_events = pd.read_csv("data/Data_eventsID.csv", delimiter="|")
    purchase_events = pd.read_csv("data/Purchase_events_ID.csv", delimiter="|")

    # I am going to transform the data_event and the purchase_event to separate the date and time
    data_events["DATE"] = data_events["TIMESTAMP_EVENT"].apply(lambda x: x.split("T")[0])
    data_events["TIME"] = data_events["TIMESTAMP_EVENT"].apply(lambda x: x.split("T")[1][:-1])

    purchase_events["DATE"] = purchase_events["DATE"].apply(lambda x: x.split(" ")[0])

    processor = Process_clients(purchase_events, data_events)
    processor.process_client_chunks()
    processed_payment_table = processor.aggregate_client_tables()
    print(processed_payment_table.head())
"""
    Process_clients.process_clients_in_chunk(Process_clients.chunks[0])
    processed_payment_table = Process_clients.aggregate_client_tables()
    print(processed_payment_table.head())"""
