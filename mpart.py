import pathlib, boto3, os, datetime, pprint, random, pytz
from yaml import load, dump, Loader, Dumper
from boto3.session import Session,Config

configFile = os.path.join(pathlib.Path(__file__).resolve().parents[0], "config", "main.yaml")

with open(configFile, mode="r") as file:
    content = file.read()
    s3Config = load(stream = content, Loader = Loader)["s3"]
    dateTimeConfig = load(stream = content, Loader = Loader)["dateTime"] 


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
        self.zone = dateTimeConfig["zone"]
        self.minutesOlder = dateTimeConfig["minutesOlder"]
        self.dryRun = dateTimeConfig["dryRun"]

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

    def get_mpart_uploads(self):
        _result = self.client.list_multipart_uploads(Bucket=self.bucketName)

        try:
            self.uploads = _result['Uploads']
        except Exception as err:
            self.uploads = None

    def list_mpart_uploads(self):
        _result = self.client.list_multipart_uploads(Bucket=self.bucketName)

        try:
            self.uploads = _result['Uploads']
        except Exception as err:
            self.uploads = None

        print(" List Multi-Part Uploads ".center(100, "="))
        pprint.pprint(self.uploads if self.uploads else "Empty")

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
                MaxParts= 1000
            )
        
        print(f" Multi-Part Upload Parts for {uploadId} ".center(100, "="))
        pprint.pprint(_response, indent=2)

    def abort_multipart_upload(self, key = None, uploadId = None):

        print(" Multi-Part Upload Aborted ".center(100, "="))
        try:
            _response = self.client.abort_multipart_upload(
                            Bucket = self.bucketName,
                            Key= key,
                            UploadId= uploadId,
                        )
            
            if(int(_response['ResponseMetadata']['HTTPStatusCode']) >= 200 and int(_response['ResponseMetadata']['HTTPStatusCode']) < 300):
                print(f"Successfully aborted {uploadId} for {key}!")
            else:
                print(f"Abort failed for {uploadId} for {key}!")

        except Exception as err:
            print(err)
    
    def abort_multipart_uploads(self, uploads = None, minutesOlder = None, zone = None, dryRun = False):

        # Precedence over method supplied parameters over object properties. Object properties are read from config files. 
        uploads = uploads or self.uploads
        minutesOlder = minutesOlder or self.minutesOlder
        zone = zone or self.zone
        dryRun = dryRun or self.dryRun

        uploadIdsToAbort = []

        print(f" List of Multi-Part Uploads Older Than {minutesOlder} Minutes To Abort ".center(100, "="))
        if not uploads:
            pprint.pprint("Empty")
            return False
        
        for upload in uploads:
            uploadCreateDateTime = upload["Initiated"]
            # print(upload["Initiated"])
            # print(upload["Initiated"].tzinfo)

            if zone:
                try:
                    # print(timeZone)
                    timeZone = pytz.timezone(zone = zone)
                    dateTime = uploadCreateDateTime.astimezone(timeZone)
                    if dateTime < timeZone.localize(datetime.datetime.now()) - datetime.timedelta(minutes = minutesOlder):
                        uploadIdsToAbort.append({upload['Key']:[upload['UploadId'], dateTime]})

                except Exception as err:
                    print("Timezone is invalid! Omitting the timezone...")
                    print(err)
            else:
                
                if uploadCreateDateTime < datetime.datetime.now() - datetime.timedelta(minutes = minutesOlder):
                    uploadIdsToAbort.append({upload['Key']:[upload['UploadId'], uploadCreateDateTime]})
                
        self.uploadIdsToAbort = None or uploadIdsToAbort
        
        pprint.pprint(self.uploadIdsToAbort)

        ################# Actual Aborts! ##############
        print(" Multi-Part Uploads Aborted ".center(100, "="))

        if dryRun:
            print("DryRun is set. No uploads are aborted...")
            return True

        if not self.uploadIdsToAbort:
            print("No matching uploads found. quiting..")
            return True

        for mpu in self.uploadIdsToAbort:
            key = None
            uploadId = None
            date = None
            for key, [uploadId, date] in mpu.items():
                print(f"Aborting upload Id: {uploadId} for key: {key} created on {date}...".ljust(60, '-'))

                try:
                    _response = self.client.abort_multipart_upload(
                        Bucket = self.bucketName,
                        Key= key,
                        UploadId= uploadId
                    )

                    if(int(_response['ResponseMetadata']['HTTPStatusCode']) >= 200 and int(_response['ResponseMetadata']['HTTPStatusCode']) < 300):
                        print(f"Successfully aborted!")
                    else:
                        print(f"Abort failed!")
                except Exception as err:
                    print(err)



if __name__ == "__main__":

    ###########################################################################
    #################### Optional Variables ############################################
    key = 'mpart_obj1'
    uploadId = '1_1000000018001_e' # Change to a real uploadId
    part = random.randbytes(1024) # This creates a 1KB part, change as desired
    partNumber = 1 # Change (or dont) with every part upload. If you dont, designated part is overwritten if it is uploaded earlier.
    zone = None # When specified overwrites the value from the config file. Set it to None to render it ineffective. For a full list of valid timezones see the output for pprint.pprint(pytz.all_timezones) 
    minutesOlder = None      # When specified overwrites the value from the config file. Set it to None to render it ineffective.
    uploads = None      # When specified overwrites the value from the config file. Set it to None to render it ineffective.
    ############################################################################
    ############################################################################

    s3 = S3(s3Config = s3Config)
    s3.connect()
    # s3.list_buckets()
    s3.get_mpart_uploads() # This should always be on
    # s3.list_mpart_uploads()
    # s3.create_multipart_upload(key = key)
    # s3.abort_multipart_upload(key = key, uploadId = uploadId)
    # s3.upload_part(key = key, uploadId = uploadId, part = part, partNumber = partNumber)
    # s3.list_parts(key = key, uploadId = uploadId)
    # s3.abort_multipart_uploads(uploads = uploads, minutesOlder = minutesOlder, zone = zone) # Run this method after list_mpart_uploads() or provide a custom list of uploads

    