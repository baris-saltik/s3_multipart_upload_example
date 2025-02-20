import pathlib, boto3, os, sys, datetime, pprint, random
from yaml import load,dump,Loader,Dumper
from boto3.session import Session,Config

configFile = os.path.join(pathlib.Path(__file__).resolve().parents[0], "config", "main.yaml")

with open(configFile, mode="r") as file:
    content = file.read()
    s3Config = load(stream = content, Loader = Loader)["s3"]


class S3(object) :
    def __init__(self, s3Config = s3Config):

        boto3.DEFAULT_SESSION

        self.endpoint_url = s3Config["endpoint_url"]
        self.aws_access_key_id = s3Config["aws_access_key_id"]
        self.aws_secret_access_key = s3Config["aws_secret_access_key"]
        self.signature_version = s3Config["signature_version"]
        self.addressing_style = s3Config["addressing_style"]
        self.connect_timeout = s3Config["connect_timeout"]
        self.read_timeout = s3Config["read_timeout"]
        self.max_attempts = s3Config["max_attempts"]
        self.bucketName = s3Config["bucketName"]
        self.use_ssl = s3Config["use_ssl"]
        self.verify = s3Config["verify"]

        self.s3Config = Config(signature_version = self.signature_version,
                               s3={'addressing_style': self.addressing_style},
                               connect_timeout = self.connect_timeout, read_timeout=self.read_timeout,
                               retries={'max_attempts': self.max_attempts}
                               )

    def connect(self):
        session = Session(aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key)
        self.s3 = session.resource('s3',
                              endpoint_url=self.endpoint_url,
                              use_ssl=True, verify=False,
                              config=self.s3Config)

        self.bucket = self.s3.Bucket(self.bucketName)

        self.client = boto3.client('s3',
                              endpoint_url=self.endpoint_url,
                              aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key,
                              aws_session_token=None,
                              use_ssl=True, verify=False,
                              config=self.s3Config)

    def list_buckets(self):
        _result = self.client.list_buckets()
        print(_result)

    def list_mpart_uploads(self):
        _result = self.client.list_multipart_uploads(Bucket=self.bucketName)
        print(" List Multi-Part Uploads ".center(100, "="))
        pprint.pprint(_result)

    def create_multipart_upload(self, key = None):
        _response = self.client.create_multipart_upload(
                Bucket = self.bucketName,
                Expires=datetime.datetime(2030, 3, 21),
                Key= key,
                Metadata={
                    'type': 'multpart uploaded'
                }
            )

        print(" Multi-Part Upload Created ".center(100, "="))
        pprint.pprint(_response, indent=2)
    
    def upload_part(self, key = None, uploadId = None, part = None, partNumber = None):
        _response = self.client.upload_part(
                Body = part,
                Bucket = self.bucketName,
                Key = key,
                PartNumber = partNumber,
                UploadId = uploadId
            )
        
        print(f" Part Upload for {uploadId}, part number: {partNumber} ".center(100, "="))
        pprint.pprint(_response, indent=2)
        

    def list_parts(self, key = None, uploadId = None):
        _response = self.client.list_parts(
                Bucket = self.bucketName,
                Key = key,
                UploadId = uploadId,
            )
        
        print(f" Multi-Part Upload Parts for {uploadId} ".center(100, "="))
        pprint.pprint(_response, indent=2)

    def abort_multipart_upload(self, key = None, uploadId = None):

        _response = self.client.abort_multipart_upload(
                        Bucket = self.bucketName,
                        Key= key,
                        UploadId= uploadId,
                    )
        
        print(" Multi-Part Upload Deleted ".center(100, "="))
        pprint.pprint(_response)


if __name__ == "__main__":

    key = 'mpart_obj1'
    uploadId = '1_1000000018001_5'
    part = random.randbytes(1024)
    partNumber = 1

    s3 = S3(s3Config = s3Config)
    s3.connect()
    # s3.list_buckets()
    # s3.create_multipart_upload(key = key)
    # s3.abort_multipart_upload(key = key, uploadId = uploadId)
    # s3.list_mpart_uploads()
    # s3.upload_part(key = key, uploadId = uploadId, part = part, partNumber = partNumber)
    # s3.list_parts(key = key, uploadId = uploadId)
    