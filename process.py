import json
import Connectors
import Transformations
import AutoML
try:
    codegentest_DBFS = Connectors.DBFSConnector.fetch(
        [], {}, "5e8eb711126ae16c96d557f2", spark, "{'url': '/Demo/PredictiveChurnTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapi9900607f95a71a87c0660d2860ef59ea', 'dbfs_domain': 'eastus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

except Exception as ex:
    print(ex)
try:
    codegentest_AutoFE = Transformations.TransformationMain.run(["5e8eb711126ae16c96d557f2"], {"5e8eb711126ae16c96d557f2": codegentest_DBFS}, "5e8eb711126ae16c96d557f3", spark, json.dumps({"FE": [{"transformationsData": {"feature_label": "State"}, "feature": "State", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "356", "mean": "", "stddev": "", "min": "AK", "max": "WY", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {}, "feature": "Account_Length", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "356", "mean": "104.24", "stddev": "40.11", "min": "3", "max": "232", "missing": "0"}}, {"transformationsData": {}, "feature": "Area_Code", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "356", "mean": "435.95", "stddev": "41.63", "min": "408", "max": "510", "missing": "0"}}, {"transformationsData": {"feature_label": "Phone"}, "feature": "Phone", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "356", "mean": "", "stddev": "", "min": "327-8732", "max": "422-8344", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {"feature_label": "Intl_Plan"}, "feature": "Intl_Plan", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "356", "mean": "", "stddev": "", "min": "no", "max": "yes", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {"feature_label": "VMail_Plan"}, "feature": "VMail_Plan", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "356", "mean": "", "stddev": "", "min": "no", "max": "yes", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {}, "feature": "VMail_Message", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "356", "mean": "7.42", "stddev": "13.24", "min": "0", "max": "46", "missing": "0"}}, {"transformationsData": {}, "feature": "Day_Mins", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "356", "mean": "178.7", "stddev": "54.22", "min": "19.5", "max": "335.5", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "Day_Calls", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "356", "mean": "100.43", "stddev": "21.34", "min": "45", "max": "163", "missing": "0"}}, {"transformationsData": {}, "feature": "Day_Charge", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "356", "mean": "30.38", "stddev": "9.22", "min": "3.32", "max": "57.04", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "Eve_Mins", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "356", "mean": "202.25", "stddev": "51.76", "min": "42.5", "max": "354.2", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "Eve_Calls", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "356", "mean": "100.22", "stddev": "21.18", "min": "48", "max": "164", "missing": "0"}}, {"transformationsData": {}, "feature": "Eve_Charge", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "356", "mean": "17.19", "stddev": "4.4", "min": "3.61", "max": "30.11", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "Night_Mins", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "356", "mean": "199.43", "stddev": "47.65", "min": "57.5", "max": "352.5", "missing": "0"}, "transformation": ""}, {
                                                                "transformationsData": {}, "feature": "Night_Calls", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "356", "mean": "100.16", "stddev": "20.45", "min": "33", "max": "157", "missing": "0"}}, {"transformationsData": {}, "feature": "Night_Charge", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "356", "mean": "8.97", "stddev": "2.14", "min": "2.59", "max": "15.86", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "Intl_Mins", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "356", "mean": "10.27", "stddev": "2.98", "min": "0.0", "max": "18.2", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "total_Mins", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "356", "mean": "590.65", "stddev": "90.31", "min": "301.5", "max": "882.2", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "Intl_Calls", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "356", "mean": "4.5", "stddev": "2.53", "min": "0", "max": "15", "missing": "0"}}, {"transformationsData": {}, "feature": "Intl_Charge", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "356", "mean": "2.77", "stddev": "0.8", "min": "0.0", "max": "4.91", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "Total_Charge", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "356", "mean": "59.32", "stddev": "10.52", "min": "25.52", "max": "90.46", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "CustServ_Calls", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "356", "mean": "1.56", "stddev": "1.39", "min": "0", "max": "7", "missing": "0"}}, {"transformationsData": {}, "feature": "Churn", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "356", "mean": "0.12", "stddev": "0.33", "min": "0", "max": "1", "missing": "0"}}, {"transformationsData": {"feature_label": "cluster_labels"}, "feature": "cluster_labels", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "356", "mean": "", "stddev": "", "min": "day_callers", "max": "vmailers", "missing": "0"}, "transformation": "String Indexer"}, {"feature": "State_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "356", "mean": "19.19", "stddev": "13.95", "min": "0.0", "max": "50.0", "missing": "0"}}, {"feature": "Phone_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "356", "mean": "177.5", "stddev": "102.91", "min": "0.0", "max": "355.0", "missing": "0"}}, {"feature": "Intl_Plan_transform", "transformation": "", "transformationsData": {}, "type": "numeric", "selected": "True", "stats": {"count": "356", "mean": "0.08", "stddev": "0.27", "min": "0", "max": "1", "missing": "0"}}, {"feature": "VMail_Plan_transform", "transformation": "", "transformationsData": {}, "type": "numeric", "selected": "True", "stats": {"count": "356", "mean": "0.26", "stddev": "0.44", "min": "0", "max": "1", "missing": "0"}}, {"feature": "cluster_labels_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "356", "mean": "2.35", "stddev": "1.71", "min": "0.0", "max": "5.0", "missing": "0"}}]}))

except Exception as ex:
    print(ex)
try:
    AutoML.functionClassification(codegentest_AutoFE, ["State", "Account_Length", "Area_Code", "Phone", "Intl_Plan", "VMail_Plan", "VMail_Message", "Day_Mins", "Day_Calls", "Day_Charge", "Eve_Mins",
                                                       "Eve_Calls", "Eve_Charge", "Night_Mins", "Night_Calls", "Night_Charge", "Intl_Mins", "total_Mins", "Intl_Calls", "Intl_Charge", "Total_Charge", "CustServ_Calls", "cluster_labels"], "Churn")

except Exception as ex:
    print(ex)
import json
import Connectors
import Transformations
import AutoML
try:
    codegentest_DBFS = Connectors.DBFSConnector.fetch(
        [], {}, "5e8eb711126ae16c96d557f2", spark, "{'url': '/Demo/PredictiveChurnTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapi9900607f95a71a87c0660d2860ef59ea', 'dbfs_domain': 'eastus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

except Exception as ex:
    print(ex)
try:
    codegentest_AutoFE = Transformations.TransformationMain.run(["5e8eb711126ae16c96d557f2"], {"5e8eb711126ae16c96d557f2": codegentest_DBFS}, "5e8eb711126ae16c96d557f3", spark, json.dumps({"FE": [{"transformationsData": {"feature_label": "State"}, "feature": "State", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "356", "mean": "", "stddev": "", "min": "AK", "max": "WY", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {}, "feature": "Account_Length", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "356", "mean": "104.24", "stddev": "40.11", "min": "3", "max": "232", "missing": "0"}}, {"transformationsData": {}, "feature": "Area_Code", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "356", "mean": "435.95", "stddev": "41.63", "min": "408", "max": "510", "missing": "0"}}, {"transformationsData": {"feature_label": "Phone"}, "feature": "Phone", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "356", "mean": "", "stddev": "", "min": "327-8732", "max": "422-8344", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {"feature_label": "Intl_Plan"}, "feature": "Intl_Plan", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "356", "mean": "", "stddev": "", "min": "no", "max": "yes", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {"feature_label": "VMail_Plan"}, "feature": "VMail_Plan", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "356", "mean": "", "stddev": "", "min": "no", "max": "yes", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {}, "feature": "VMail_Message", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "356", "mean": "7.42", "stddev": "13.24", "min": "0", "max": "46", "missing": "0"}}, {"transformationsData": {}, "feature": "Day_Mins", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "356", "mean": "178.7", "stddev": "54.22", "min": "19.5", "max": "335.5", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "Day_Calls", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "356", "mean": "100.43", "stddev": "21.34", "min": "45", "max": "163", "missing": "0"}}, {"transformationsData": {}, "feature": "Day_Charge", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "356", "mean": "30.38", "stddev": "9.22", "min": "3.32", "max": "57.04", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "Eve_Mins", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "356", "mean": "202.25", "stddev": "51.76", "min": "42.5", "max": "354.2", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "Eve_Calls", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "356", "mean": "100.22", "stddev": "21.18", "min": "48", "max": "164", "missing": "0"}}, {"transformationsData": {}, "feature": "Eve_Charge", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "356", "mean": "17.19", "stddev": "4.4", "min": "3.61", "max": "30.11", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "Night_Mins", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "356", "mean": "199.43", "stddev": "47.65", "min": "57.5", "max": "352.5", "missing": "0"}, "transformation": ""}, {
                                                                "transformationsData": {}, "feature": "Night_Calls", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "356", "mean": "100.16", "stddev": "20.45", "min": "33", "max": "157", "missing": "0"}}, {"transformationsData": {}, "feature": "Night_Charge", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "356", "mean": "8.97", "stddev": "2.14", "min": "2.59", "max": "15.86", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "Intl_Mins", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "356", "mean": "10.27", "stddev": "2.98", "min": "0.0", "max": "18.2", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "total_Mins", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "356", "mean": "590.65", "stddev": "90.31", "min": "301.5", "max": "882.2", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "Intl_Calls", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "356", "mean": "4.5", "stddev": "2.53", "min": "0", "max": "15", "missing": "0"}}, {"transformationsData": {}, "feature": "Intl_Charge", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "356", "mean": "2.77", "stddev": "0.8", "min": "0.0", "max": "4.91", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "Total_Charge", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "356", "mean": "59.32", "stddev": "10.52", "min": "25.52", "max": "90.46", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "CustServ_Calls", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "356", "mean": "1.56", "stddev": "1.39", "min": "0", "max": "7", "missing": "0"}}, {"transformationsData": {}, "feature": "Churn", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "356", "mean": "0.12", "stddev": "0.33", "min": "0", "max": "1", "missing": "0"}}, {"transformationsData": {"feature_label": "cluster_labels"}, "feature": "cluster_labels", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "356", "mean": "", "stddev": "", "min": "day_callers", "max": "vmailers", "missing": "0"}, "transformation": "String Indexer"}, {"feature": "State_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "356", "mean": "19.19", "stddev": "13.95", "min": "0.0", "max": "50.0", "missing": "0"}}, {"feature": "Phone_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "356", "mean": "177.5", "stddev": "102.91", "min": "0.0", "max": "355.0", "missing": "0"}}, {"feature": "Intl_Plan_transform", "transformation": "", "transformationsData": {}, "type": "numeric", "selected": "True", "stats": {"count": "356", "mean": "0.08", "stddev": "0.27", "min": "0", "max": "1", "missing": "0"}}, {"feature": "VMail_Plan_transform", "transformation": "", "transformationsData": {}, "type": "numeric", "selected": "True", "stats": {"count": "356", "mean": "0.26", "stddev": "0.44", "min": "0", "max": "1", "missing": "0"}}, {"feature": "cluster_labels_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "356", "mean": "2.35", "stddev": "1.71", "min": "0.0", "max": "5.0", "missing": "0"}}]}))

except Exception as ex:
    print(ex)
try:
    AutoML.functionClassification(codegentest_AutoFE, ["State", "Account_Length", "Area_Code", "Phone", "Intl_Plan", "VMail_Plan", "VMail_Message", "Day_Mins", "Day_Calls", "Day_Charge", "Eve_Mins",
                                                       "Eve_Calls", "Eve_Charge", "Night_Mins", "Night_Calls", "Night_Charge", "Intl_Mins", "total_Mins", "Intl_Calls", "Intl_Charge", "Total_Charge", "CustServ_Calls", "cluster_labels"], "Churn")

except Exception as ex:
    print(ex)