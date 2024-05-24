# how to set everything up
1. Clone the repository
2. Install the required packages in requirements.txt
3. Create folder "processed_data" in the root of the project
4. Add the "processed_events.csv" file and "processed_purchase_events.csv" to the "processed_data" folder


# If you want to remake the dataset of "processed_events.csv" you can run the following steps
1. You first must have the file structure of the clients in place
    You can do that by using the jupyter notebook "save_clients_in_folder_structure_updated.ipynb"
2. Run  "process_client_tree.py" to create the "processed_events.csv" file with the function
    chunks_to_process = process_clients.setup_chunks_from_client_list(client_id_list, chunk_size=100)
    process_clients.process_client_purchases_to_event_multiprocessing()
3. Now the file structure is updated with the amount of times a purchase row is seen for every event row


# Explanation of the Folders
1. benchmark_data
   - Contains the data files that were used to benchmark the model, this data is used in rewardRandomForest.py
2. models
   - Contains the trained model for random forest to predict the rewards of rankings
3. processed_data
   4. Contains the data that we have created from preprocessing. This data can be recreated with the python files
4. client_data
   - contains data about each client. Each folder in this directory is a client. It contains the purchase and datat events for that client.

# Explanation of the Files. In chronological order of usage. Use them to set the data up
### Important to note is that we instantly use the the processed_events and processed_events csv file. We lost the file
### We changed the names of the files to processed_events.csv and processed_purchase_events.csv

### so Just rename the data events and purchase events to work with the correct naming. 
1. [add_category_to_purchases.ipynb](add_category_to_purchases.ipynb)
    - Adding the category of products to the purchases
2. [weather_data.ipynb](weather_data.ipynb)
    - Exploration of the weather data. Adds the weather data to the events table
3. [save_clients_in_folder_structure_updated.ipynb](save_clients_in_folder_structure_updated.ipynb)
    - Saving the clients in a folder structure
4. [Process_Client_add_events_to_purchases.py](Process_Client_add_events_to_purchases.py)
    - Adding amount of times the person  has seen the product in last 7 and 30 days when a purchase is made. This data eventually not used
5. [process_client_tree.py](process_client_tree.py)
    - Creating the processed_events.csv file with the amount of times a purchase row is seen for every event row. 
    - It works with multiprocessing
6. [randomForestModelCreatorWeather.ipynb](randomForestModelCreatorWeather.ipynb)
    - Creating the random forest model to predict the rewards of rankings. This runs the model and saves it

7. [rfm_score_user.ipynb](rfm_score_user.ipynb)
    - Creating the RFM score for each user. This is used to create the benchmark data. This is needed for the next step.
    - This creates the benchmark data for the current ranking of exsell and takes a subset of it, and 
    - puts it in the benchmark_data folder

8. [benchmark.ipynb](%20benchmark.ipynb)
    - creating the benchmark data to be processed and evaluated by our randomforest ranking evaluator
    - Creates the benchmark for the most bought products and saves it in the benchmark_data folder

9. [rewardRandomForest.py](rewardRandomForest.py)
    - Uses the data created and addd to the benchamrk data folders and evaluates them. Important to first run
    - the benchmark.ipynb and rfm_score_user.ipynb to have the benchmark data in place

# Failed attempt for neural network
1. [NN_MODEL.ipynb](NN_MODEL.ipynb)
    - Attempt to create a neural network to predict the rewards of rankings. This was not successful


# Exploration files that do not be needed to run the system
1. [causal inference.ipynb](causal%20inference.ipynb)
   - Exploration of the data
2. [Explore_data_contexts.ipynb](Explore_data_contexts.ipynb)
   - Exploration of the data. Seeing the contexts
3. [segmentation.ipynb](segmentation.ipynb)
   - Exploring of data relating to RFM
4. [evaluator.ipynb](evaluator.ipynb)
   - Data exploration of the Random forest model. 
   - Also contextual bandit for predicting the RFM group for users
5. [visualization_of_graphs.ipynb](visualization_of_graphs.ipynb)
    - Visualization of the results made by the evaluation of ranking vs benchmark of random forest model