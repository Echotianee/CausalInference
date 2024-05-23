import pickle
import json

class RewardPredictor():
    def __init__(self, model_location="model/reward_predictor_model.pkl"):
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

    def predict_ranking(self, product_ranking, features):
        """
        predicts the ranking of the products based on the features
        It returns a list of products with their ranking

        :param product_ranking: list of products
        :param features: features to predict the ranking
        :return: list of products with their ranking
        """
        ranking = []
        for product in product_ranking:
            reward = self.predict_reward(features + [product])
            ranking.append((product, reward))
        return ranking


if __name__ == "__main__":
    reward_predictor = RewardPredictor()

    with open("processed_data/user_recommendations_types.json") as f:
        clients_per_recommendation_type = json.load(f)


    #print(reward_predictor.predict_reward(features))
    #print(reward_predictor.predict_ranking(["product1", "product2", "product3"], features))



