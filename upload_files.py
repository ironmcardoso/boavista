import cloudstorage as gcs
from google.cloud import storage
from google.oauth2 import service_account

from os import listdir
from os.path import isfile, join


bucket_name = 'bv-data'
bucket_folder = 'data/'
local_folder = "C:\\Users\\iron\\Downloads\\bv\\"

storage_client = storage.Client.from_service_account_json("iron-985f2d4ba8a9.json")

bucket = storage_client.get_bucket(bucket_name)

def upload(bucket_name, bucket_folder, local_folder):

    file_list = []
    for file in listdir(local_folder):
        if isfile(join(local_folder, file)):
            file_list.append(file)

    for file in file_list:
        local_file = local_folder + file
        blob = bucket.blob(bucket_folder + file)
        blob.upload_from_filename(local_file)
        
    return f'Uploaded {file_list} to "{bucket_name}" bucket.'

print(upload(bucket_name, bucket_folder, local_folder))
