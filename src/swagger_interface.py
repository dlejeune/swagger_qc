from wsgiref import headers
import PySimpleGUI as sg
from swagger_extractor import Extractor
import os

sg.theme('Default1')   # Add a touch of color


layout = [  [sg.Text('Enter the analysis ID')], 
            [sg.InputText("e4171b10903f4299a4de37e7ebd865c1", key = "analysis_id")],
            [sg.Text("Choose location to save output: "), sg.FolderBrowse(key = "FileChoose", target="OutputLocation")],
            [sg.In(os.getcwd(), key="OutputLocation", disabled=True)],
            #[sg.Text('Metrics')],
            #[sg.Combo(metric_values, key="metrics")],
            #[sg.InputText(key = "metrics")],
            #[sg.Text('Data Fields')], 
            #data_field_widgets,
            [sg.Output(size=(70,10), key='-OUTPUT-')],
            [sg.Button('Run'), sg.Button('Close')],
            [sg.StatusBar("Ready", key="status")]]

# Create the Window
window = sg.Window('Swagger Extractor', layout)
# Event Loop to process "events" and get the "values" of the inputs



while True:
    event, values = window.read()
    if event == sg.WIN_CLOSED or event == 'Close': # if user closes window or clicks cancel
        break
    if event == "Run":

        window.Element("status").update("Running")
        analysis_id = values["analysis_id"]
 
        extractor = Extractor(analysis_id)

        out_folder = values["OutputLocation"]
        
        window.perform_long_operation(lambda:
        extractor.qc_standard(out_folder),
        '-OPERATION DONE-')
        
    if event == "-OPERATION DONE-":
        window.Element("status").update("Done")
        
        

window.close()