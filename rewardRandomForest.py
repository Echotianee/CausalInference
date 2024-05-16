import pickle

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
        features_to_predict_with = [feature for feature in possible_features if feature in possible_features]
        return self.model.predict(features_to_predict_with)



