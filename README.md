# how to set everything up
1. Clone the repository
2. Install the required packages in requirements.txt
3. Create folder "processed_data" in the root of the project
4. Add the "processed_events.csv" file and "processed_purchase_events.csv" to the "processed_data" folder
5. Run jupyter notebook save_clients_in_folder_structure_updated.ipynb


# If you want to remake the dataset of "processed_events.csv" you can run the following steps
1. You first must have the file structure of the clients in place
    You can do that by using the jupyter notebook "save_clients_in_folder_structure.ipynb"
2. Run  "process_client_tree.py" to create the "processed_events.csv" file with the function
    chunks_to_process = process_clients.setup_chunks_from_client_list(client_id_list, chunk_size=100)
    process_clients.process_client_purchases_to_event_multiprocessing()
3. Now the file structure is updated with the amount of times a purchase row is seen for every event row