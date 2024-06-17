from google.cloud import storage

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Downloaded storage object {} from bucket {} to local file {}.".format(
            source_blob_name, bucket_name, destination_file_name
        )
    )

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

if __name__ == '__main__':
    
    # เลือกว่าจะอัปโหลดหรือดาว์นโหลดไฟล์ที่อยู่ใน bucket
    upload = input("Upload (u) or Download (d)? : ")
 
    file_name = input("What’s local name to : ")
    gcs_file_name = input("What's gcs file name : ")
 
    # bucket ที่เราจะเก็บ
    bucket_name = 'test-bucket-terminal'
    
    # ถ้าเกิด upload หรือ u ให้เอาไฟล์ไปเก็บใน bucket ที่ตั้งค่า
    if upload.lower() == "upload" or upload.lower() == "u":
        upload_blob(
            bucket_name=bucket_name,
            source_file_name=file_name,
            destination_blob_name=gcs_file_name
        )

    # ถ้าเกิด download หรือ d ให้โหลด data ใน bucket ไปเก็บใน shell
    elif upload.lower() == "download" or upload.lower() == "d":
        download_blob(
            bucket_name=bucket_name,
            source_blob_name=gcs_file_name,
            destination_file_name=file_name
        )
    else:
        print('Please input Upload or Download')