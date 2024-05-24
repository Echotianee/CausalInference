import pickle
import json
import os
import pandas as pd

class RewardPredictor():
    def __init__(self, model_location="models/reward_predictor_model_weather_0.pkl"):
        self.model_location = model_location
        self.model = self.load_model(model_location)

    def load_model(self, model_location):
        """
        loads the model from the model location
        :return: RandomForestPredictor model
        """
        with open(model_location, "rb") as pickle_file:
            return pickle.load(pickle_file)

    def predict_reward(self, features):
        """
        predicts the reward based on the features
        It returns a value between 0 and 1


        :param features: features to predict the reward
        :return: reward
        """
        possible_features = ["PRICE", "PROPOSITION", "USER_CLIENT_NUMBER", "USER_SESSION_ID", "PROMOTION_LABEL", "PAGE_NAME", "PAGE_SECTION", "PAGE_SECTION_POSITION", "PROMOTION_PRICE", "PRODUCT_TYPE", "DEVICE_INFO_BRAND", "DEVICE_INFO_TYPE", "DEVICE_INFO_BROWSER", "USER_SALES_GROUP", "USER_SEGMENT", "USER_SALES_DISTRICT", "USER_PROMOTIONS_ALLOWED"]
        possible_features = [
                            "PRICE",
                            "PROPOSITION",
                            "USER_CLIENT_NUMBER",
                            "USER_SESSION_ID",
                            "PROMOTION_LABEL",
                            "PAGE_SECTION",
                            "PROMOTION_PRICE",
                            "DEVICE_INFO_BRAND",
                            "DEVICE_INFO_TYPE",
                            "DEVICE_INFO_BROWSER",
                            "USER_SALES_GROUP",
                            "USER_SEGMENT",
                            "USER_SALES_DISTRICT",
                            "USER_PROMOTIONS_ALLOWED",
                            "temperature",
                            "precipcover",
                            "precip",
                            "temperature_lead_1",
                            "precipitation_coverage_lead_1",
                            "precipitation_amount_lead_1",
                            "temperature_lead_2",
                            "precipitation_coverage_lead_2",
                            "precipitation_amount_lead_2",
                            "temperature_lead_3",
                            "precipitation_coverage_lead_3",
                            "precipitation_amount_lead_3",
                            "temperature_lead_4",
                            "precipitation_coverage_lead_4",
                            "precipitation_amount_lead_4",
                            "total_spend_on_category_product",
                            "total_spend_on_product",
                            "day_of_week"
                        ]


        features_to_predict_with = [feature for feature in features if feature in possible_features]
        return self.model.predict(features_to_predict_with)

    def predict_ranking_complete_dataset(self):
        """
        predicts the ranking of the products based on the features
        It returns a list of products with their ranking

        :param product_ranking: list of products
        :param features: features to predict the ranking
        :return: list of products with their ranking
        """
        rewards = {

        }
        for folder in os.listdir("benchmark_data"):
            if ".DS_Store" in folder:
                continue
            rewards[folder] = {}
            for file in os.listdir("benchmark_data/" + folder):
                if ".DS_Store" in folder:
                    continue
                rewards[folder][file] = 0
                df = pd.read_csv("benchmark_data/" + folder + "/" + file, sep="|")
                transformed_df = self.preprocessing_data(df)
                for index, row in transformed_df.iterrows():
                    reward = self.predict_reward(row)
                    rewards[folder][file] += reward

        return rewards


    def preprocessing_data(self, event_data_to_analyze):
        """
        preprocesses the data
        :param features: features to preprocess
        :return: preprocessed features
        """


        # Replace categorical values using map
        transformation_dict = self.data_events_to_categories_dict_creation(event_data_to_analyze)
        inverse_transformation_dict = self.get_inverse_transformation_dict(transformation_dict)

        transformed_event_df = self.transform_df_to_categories(event_data_to_analyze, transformation_dict,
                                                               to_categories=True)


        return transformed_event_df

    def data_events_to_categories_dict_creation(self, data_events):
        """
        Create a dictionary with the columns as keys and the values as a dictionary with the unique values as keys and the category number as values
        Args:
            data_events:

        Returns:

        """
        transformation_dict = {}
        for col in ["PAGE_SECTION", "DEVICE_INFO_BRAND", "DEVICE_INFO_TYPE",
                    "DEVICE_INFO_BROWSER", "USER_SALES_GROUP", "USER_SEGMENT", "USER_SALES_DISTRICT",
                    ]:
            print(data_events.columns)
            unique_col_values = data_events[col].unique()

            category_number = 0
            transformation_dict[col] = {}
            for col_value in unique_col_values:
                transformation_dict[col][col_value] = category_number
                category_number += 1
        return transformation_dict

    def get_inverse_transformation_dict(self, transformation_dict):
        """
        Create a dictionary with the columns as keys and the values as a dictionary with the category number as keys and the unique values as values
        Args:
            transformation_dict:

        Returns:

        """
        # inverse of transformation dict
        inverse_transformation_dict = {}
        for col_name, dict_vals in transformation_dict.items():
            inverse_transformation_dict[col_name] = {}
            for dict_val, cat_num in dict_vals.items():
                inverse_transformation_dict[col_name][cat_num] = dict_val

        return inverse_transformation_dict

    def transform_df_to_categories(self, events_table, inverse_or_transformation_dict, to_categories=True):
        """
        Replace categorical values using map based on the inverse or transformation dict that is passed from the function
        Args:
            events_table:
            inverse_or_transformation_dict:
            to_categories:

        Returns:

        """

        if to_categories:
            for column, mapping in inverse_or_transformation_dict.items():
                events_table[column] = events_table[column].map(lambda x: mapping[x])
        else:
            for column, mapping in inverse_or_transformation_dict.items():
                events_table[column] = events_table[column].map(lambda x: mapping[x])
        return events_table




if __name__ == "__main__":
    reward_predictor = RewardPredictor()

    print(reward_predictor.predict_ranking_complete_dataset())


    #print(reward_predictor.predict_reward(features))
    #print(reward_predictor.predict_ranking(["product1", "product2", "product3"], features))



