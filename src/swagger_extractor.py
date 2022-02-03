
import pandas as pd
import requests 
import datetime as dt
from os import path

class Extractor():

    alignment_fields = ["PCT_ADAPTER", 'PCT_PF_READS_ALIGNED', 'PCT_CHIMERAS']

    rnaseq_fields = [
        "MEDIAN_CV_COVERAGE", 
        "MEDIAN_3PRIME_BIAS",
        "MEDIAN_5PRIME_BIAS",
        "PCT_MRNA_BASES",
        "PCT_RIBOSOMAL_BASES",
        "PCT_INTERGENIC_BASES",
        "PCT_INTRONIC_BASES",

        ]

    wmgrna_fields = ["mean"]

    duplication_fields = ["PERCENT_DUPLICATION"]
    
    def __init__(self, analysis_id):
        self.analysis_id = analysis_id

        self.analysis_url = "http://192.168.130.20:3882/analysis_data/api/v1/data/retrieve?"
        self.sample_url = "http://192.168.130.20:3881/sample_tracker/api/v1/sample/info?"


    def convert_json_to_df(self, json,  metric, row_number = (0)):

        json_df = pd.DataFrame()

        try:
            for line in json: 
                ## The data[2] must only be that for when metrics is alignments
                items_arr = []
                for data_row in line["data"]:
                    if data_row["row"] in row_number:
                        items_arr.extend(data_row["items"])
                        
                tmp = pd.DataFrame(items_arr)
                tmp['sample_id'] = line['sample_id']
                json_df  = pd.concat([json_df , tmp])
        except Exception as e:
            print("An error occured: {}".format(str(e)))

        json_df["metric"] = metric
        json_df  = json_df.pivot(index = 'sample_id' , columns= ["metric", 'key'], values= 'value')
        json_df["analysis_id"] = self.analysis_id 

        return json_df.copy()


    def get_alignment_metrics(self):
        request_url = "{}analysis_ids={}&metrics_names=AlignmentSummaryMetrics&data_fields={}".format(self.analysis_url, self.analysis_id, "%2C".join(self.alignment_fields))

        alignment_json = self.perform_request(request_url)
        self.alignment_df = self.convert_json_to_df(alignment_json, metric = "AlignmentSummaryMetrics", row_number = (3,))

    def get_rnaseq_metrics(self):
        request_url = "{}analysis_ids={}&metrics_names=RnaSeqMetrics&data_fields={}".format(self.analysis_url, self.analysis_id, "%2C".join(self.rnaseq_fields))

        rnaseq_json = self.perform_request(request_url)
        self.rnaseq_df = self.convert_json_to_df(rnaseq_json, metric = "RnaSeqMetrics", row_number = (1,))
        pass

    def get_wmgrna_metrics(self):
        request_url = "{}analysis_ids={}&metrics_names=WmgRnaInserts&data_fields={}".format(self.analysis_url, self.analysis_id, "%2C".join(self.wmgrna_fields))

        wmgrna_json = self.perform_request(request_url)
        self.wmgrna_df = self.convert_json_to_df(wmgrna_json, metric = "WmgRnaInserts", row_number = (1,))

    def get_duplication_metrics(self):
        request_url = "{}analysis_ids={}&metrics_names=DuplicationMetrics&data_fields={}".format(self.analysis_url, self.analysis_id, "%2C".join(self.duplication_fields))

        duplication_json = self.perform_request(request_url)
        self.duplication_df = self.convert_json_to_df(duplication_json, metric = "DuplicationMetrics", row_number = (1,))
    
    ## TODO: Clean up all these functions

    def join_exported_metrics(self):
        
        self.final_df = self.alignment_df.join(self.rnaseq_df.drop("analysis_id", axis=1), on="sample_id", how = "inner")

        self.final_df = self.final_df.join(self.wmgrna_df.drop("analysis_id", axis=1), on="sample_id", how = "inner")

        self.final_df = self.final_df.join(self.duplication_df.drop("analysis_id", axis=1), on="sample_id", how = "inner")

        pass

    def export_df(self, df, descriptor="", filename = None):
        filename = path.join(filename, "{}_{}_{}.xlsx".format(str(dt.date.today()), self.analysis_id, descriptor))

        self.log("Exporting {} data to {}".format(descriptor, filename))

        df.to_excel(filename)

    

    def perform_request(self, request_url):
        self.log("Sending request with url {}".format(request_url))
        r = requests.get(request_url)
        request_json = r.json()
        return request_json

        pass
    def log(self, message):
        print(message)
        pass

    
    def extract_join_save_allmetrics(self, output_file = None):
        self.get_alignment_metrics()
        self.get_rnaseq_metrics()
        self.get_duplication_metrics()
        self.get_wmgrna_metrics()

        self.join_exported_metrics()

        self.export_df(self.final_df, filename = output_file)

        pass



# e = Extractor("e4171b10903f4299a4de37e7ebd865c1")

# e.perform_request("http://192.168.130.20:3882/analysis_data/api/v1/data/retrieve?analysis_ids=e4171b10903f4299a4de37e7ebd865c1&metrics_names=AlignmentSummaryMetrics&data_fields=PCT_ADAPTER%2CPCT_PF_READS_ALIGNED%2CPCT_CHIMERAS")

# e.convert_json_to_df(jj, row_number=(2))

# def get_and_clean_data(base_url, analysis_id, metric_names, data_fields):
#     #Enter the api_URL below to retrieve relevant data.

    
#     api_url = "{}analysis_ids={}&metrics_names={}&data_fields={}".format(base_url, analysis_id, metric_names, data_fields)
#     print(api_url)
#     #Pulling the data from the given URL and storing it in json format as the 'storage' variable
#     r = requests.get(api_url)
#     storage = r.json()

#     print("Hello")
#     #QC on the type of data imported, number of samples, pretty-printed data
#     #print(type(storage))
#     ##print(len(storage))
#     #print(json.dumps(storage, indent=4, sort_keys=True))

#     #opening an empty dataframe to work on
#     df = pd.DataFrame()

#     # Iterating over the different lines in the dataframe - each line is a new sample (list of dictionaries)
#     #tmp is a temporary file to build the new dataframe
#     # we want every 'line' and then the first indent of 'data' and then all the items. 
#     #We build up the array with each line as a sample id, 'key' as a column
#     try:
#         for line in storage: 
#             ## The data[2] must only be that for when metrics is alignments
#             tmp = pd.DataFrame(line['data'][2]['items'])
#             tmp['sample_id'] = line['sample_id']
#             df = pd.concat([df, tmp])
#     except TypeError as t_error:
#         print("A TypeError occured while converting the json to a dataframe. This normally occurs when insufficient paramters were specified. Please check all the values were correctly filled out.")
#         print("The actual error message was: \n{}".format(str(t_error)))
#     except KeyError as e:
#         print("A KeyError occured while converting the json to a dataframe.")
#         print("The actual error message was: \n{}".format(str(e)))


#     #Need to pivot the table to get the keys as columns and index as sample_id

  
#     pivot = df.pivot(index = 'sample_id' , columns= 'key', values= 'value')
#     pivot["analysis_id"] = analysis_id
        
#     pivot.to_csv(storage[0]['metrics']+'.csv')
