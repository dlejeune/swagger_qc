
from urllib import request
import pandas as pd
import requests 
import datetime as dt
from os import path
import os

class Extractor():

    request_params= {
        "RnaSeqMetrics": [
            "MEDIAN_CV_COVERAGE", 
            "MEDIAN_3PRIME_BIAS",
            "MEDIAN_5PRIME_BIAS",
            "PCT_MRNA_BASES",
            "PCT_RIBOSOMAL_BASES",
            "PCT_INTERGENIC_BASES",
            "PCT_INTRONIC_BASES",
            "PCT_CORRECT_STRAND_READS"
        ],

        "AlignmentSummaryMetrics": [
            "PCT_ADAPTER", 
            'PCT_PF_READS_ALIGNED', 
            'PCT_CHIMERAS'
        ],

        "WmgRnaInserts": [
            "mean",
        ],

        "DuplicationMetrics": [
            "PERCENT_DUPLICATION",
        ]


    }

    df_list = []
    
    def __init__(self, analysis_id):
        self.analysis_id = analysis_id

        self.analysis_url = "http://192.168.130.20:3882/analysis_data/api/v1/data/retrieve"
        self.sample_url = "http://192.168.130.20:3881/sample_tracker/api/v1/sample/info?"


    def get_metric_df(self, metric, datafields = None):
        payload = {}
        payload["analysis_ids"] = self.analysis_id
        if not datafields:
            payload["data_fields"] = ",".join(self.request_params[metric])
        else:
            # Need to determine format for this param
            payload["data_fields"] = datafields
        
        payload["metrics_names"] = metric

        self.log("Sending request for {} with data_fields: {}".format(metric, payload["data_fields"]))
        request = requests.get(self.analysis_url, params=payload)
        request_json = request.json()
        
        

        json_df =  pd.json_normalize(request_json, ["data", ["items"]], ["sample_id", "metrics", "analysis_id", ["data", "row"]])

        if metric == "AlignmentSummaryMetrics":
            json_df = json_df.loc[json_df["data.row"] == 1, :]

        json_df = json_df.drop("data.row", axis=1)

       
        json_df  = json_df.pivot(index = ["analysis_id", 'sample_id'] , columns= ["metrics", 'key'], values= 'value')

        
        return json_df.copy()


    def get_sample_names(self, sample_ids):

        # Get list of samples

        final_json = []
        request_size = 20
        self.log("Fetching sample names")
        # this is so jank :'(
        
        split_sample_ids = lambda sample_ids, request_size: [sample_ids[i:i+request_size] for i in range(0, len(sample_ids), request_size)]

        batches = split_sample_ids(sample_ids, request_size)
        for i, subset in enumerate(batches):
            self.log("Fetching batch of sample names {} of {}".format(i+1, len(batches)))
            payload = {"sample_ids":",".join(subset)}
            r = requests.get("http://192.168.130.20:3881/sample_tracker/api/v1/sample/info", params=payload)
            final_json.extend(r.json())

        name_df = pd.DataFrame(final_json)
        name_df["analysis_id"] = self.analysis_id
        name_df = name_df.rename(columns={"uuid": "sample_id"})
        name_df = name_df.loc[:, ["name", "sample_id", "analysis_id"]]
        name_df = name_df.set_index(["analysis_id", "sample_id"])
        name_df.columns = pd.MultiIndex.from_product([['Metadata'], name_df.columns])
        return name_df.copy()


    def join_exported_metrics(self):
        self.out_df = self.df_list[0]

        for i in range(1, len(self.df_list)-1):
            self.out_df = self.out_df.join(self.df_list[i], on = ["analysis_id", "sample_id"], how="inner")

        self.join_sample_names(self.out_df.index.get_level_values(1).to_list())

    def export_df(self, df, descriptor="", filename = None):
        filename = path.join(filename, "{}_{}_{}.xlsx".format(str(dt.date.today()), self.analysis_id, descriptor))

        self.log("Exporting {} data to {}".format(descriptor, filename))

        df.to_excel(filename)

    def join_sample_names(self, sample_ids):
        self.log("Joining sample names")
        name_df = self.get_sample_names(sample_ids)
        self.log("Gluing sample names onto existing Dataframe")
        
        self.out_df = self.out_df.join(name_df)
        pass


    def log(self, message):
        print(message)
        pass

    def get_metrics(self, metrics, datafields = None):
        for metric in metrics:
            self.df_list.append(self.get_metric_df(metric, datafields))
        
    def qc_standard(self, filename):
        self.log("Fetching and transforming data")
        self.get_metrics(self.request_params.keys())
        self.log("Joning all metrics together")
        self.join_exported_metrics()
        self.export_df(self.out_df.copy(), filename=filename)

    
    def extract_join_save_allmetrics(self, output_file = None):
        self.get_alignment_metrics()
        self.get_rnaseq_metrics()
        self.get_duplication_metrics()
        self.get_wmgrna_metrics()

        self.join_exported_metrics()

        self.export_df(self.final_df, filename = output_file)

        pass