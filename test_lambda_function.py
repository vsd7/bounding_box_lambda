import unittest
import os
import lambda_function
from moto import mock_s3
import boto3


@mock_s3
class LambdaFunction(unittest.TestCase):
    DIR_PATH = os.path.dirname(os.path.realpath(__file__))
    IMAGEPATH = "u-1608292924392/f-1616075246357/ENSG00000000003_4109_23_H11_2_blue.tif"
    JSONPATH = "u-1608292924392/f-1616075246357/meta/ENSG00000000003_4109_23_H11_2_blue_nuc-det.json"
    IMAGE1 = "ENSG00000000003_4109_23_H11_2_blue.tif"
    JSONFILE = "ENSG00000000003_4109_23_H11_2_blue_nuc-det.json"
    IMAGEEXTENSION = "tif"
    SUFFIX = "_nuc-det"
    BUCKET = "test_bucket"
    S3PATH = "u-1608292924392/f-1616075246357/burnedimages/"
    AWSACCESSID = "fakeid"
    AWSSECRETKEY = "fakesecret"
    RDS_HOSTNAME = "RDS_HOSTNAME"
    RDS_USERNAME = "RDS_USERNAME"
    RDS_DB_NAME = "RDS_DB_NAME"
    RDS_PASSWORD = "RDS_PASSWORD"
    CONST_TEMP = "/tmp/"  # NOSONAR

    def setUp(self):
        client = boto3.client(
            "s3",
            region_name="us-east-1"
        )

        client.create_bucket(Bucket=LambdaFunction.BUCKET)

    def tearDown(self):
        self.remove_bucket(LambdaFunction.BUCKET)

    @staticmethod
    def remove_bucket(bucket_name):
        s3_bucket = boto3.resource("s3").Bucket(bucket_name)
        s3_bucket.objects.all().delete()
        s3_bucket.delete()

    @staticmethod
    def get_sqs_event():
        return {
            "Records": [
                {
                    "messageId": "19dd0b57-b21e-4ac1-bd88-01bbb068cb78",
                    "receiptHandle": "MessageReceiptHandle",
                    "body": '{\"DownloadRequestId\": \"Req-1616075432009\", \"Bucket\": \"t-72bb13bc-0107-7334-9f4b-f86add651c06\", \"JsonFilePath\": \"u-1608292924392/f-1616075246357/meta/ENSG00000000003_4109_23_H11_2_blue_nuc-det.json\", \"ImageFilePath\": \"u-1608292924392/f-1616075246357/ENSG00000000003_4109_23_H11_2_blue.tif\", \"FolderId\": 423, \"DownloadRequired\": 1}',
                    "attributes": {
                        "ApproximateReceiveCount": "1",
                        "SentTimestamp": "1523232000000",
                        "SenderId": "123456789012",
                        "ApproximateFirstReceiveTimestamp": "1523232000001",
                    },
                    "messageAttributes": {},
                    "md5OfBody": "{{{md5_of_body}}}",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:eu-central-1:123456789012:MyQueue",
                    "awsRegion": "us-east-1",
                }
            ]
        }

    @staticmethod
    def upload_file_to_s3():
        boto3.client("s3").upload_file(
            LambdaFunction.CONST_TEMP + LambdaFunction.IMAGEPRED,
            LambdaFunction.BUCKET,
            LambdaFunction.S3PATH + LambdaFunction.IMAGEPRED,
        )

    @staticmethod
    def download_image_file_from_s3():
        return boto3.client("s3").download_file(
            LambdaFunction.BUCKET,
            LambdaFunction.IMAGEPATH,
            LambdaFunction.CONST_TEMP + LambdaFunction.IMAGE1,
        )

    @staticmethod
    def download_json_file_from_s3():
        return boto3.client("s3").download_file(
            LambdaFunction.BUCKET,
            LambdaFunction.JSONPATH,
            LambdaFunction.CONST_TEMP + LambdaFunction.JSONFILE,
        )

    def test_lambda_hanlder(self):
        event = self.get_sqs_event()
        lambda_function.lambda_handler(event, None)
        self.download_image_file_from_s3()
        self.download_json_file_from_s3()
        self.upload_file_to_s3()


if __name__ == "__main__":
    unittest.main()
