import json
import os
import os.path
from os import path
import logging
import pymysql
import boto3
import cv2
from PIL import ImageColor

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3',aws_access_key_id=os.environ['AWSACCESSID'],aws_secret_access_key=os.environ['AWSSECRETKEY'])
try:
    conn = pymysql.connect(host=os.environ['RDS_HOSTNAME'], user=os.environ['RDS_USERNAME'], passwd=os.environ['RDS_PASSWORD'], db=os.environ['RDS_DB_NAME'], connect_timeout=5, autocommit=True)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()
logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")
def lambda_handler(event, context):
    for record in event['Records']:
        obj = json.loads(record["body"])
        logger.info(obj)
        imagepath=obj['ImageFilePath']
        jsonpath=obj['JsonFilePath']
        image=imagepath.split('/')[-1]
        jsonfile=jsonpath.split('/')[-1]
        imageextension=image.split('.')[1]
        suffix=jsonfile.split('.')[0][-8:]
        bucket=obj['Bucket']
        s3path=imagepath.rpartition('/')[0]
        s3path=s3path+'/burnedimages/'
        logger.info(s3path)
        logger.info(imagepath)
        logger.info(image)
        logger.info(imageextension)
        logger.info(jsonpath)
        logger.info(jsonfile)
        logger.info(suffix)
        CONST_TEMP='/tmp/'
        label_dict = {}
        color_dict = {}
        try:
            s3_client.download_file(bucket, imagePath,
                                    CONST_TEMP+image)
            logger.info("path "+str(path.exists(CONST_TEMP+image)))
            output_image = cv2.imread(CONST_TEMP+image)
            
            s3_client.download_file(bucket, jsonpath,
                                    CONST_TEMP+jsonfile)
            logger.info("path "+str(path.exists(CONST_TEMP+jsonfile)))
            with open(CONST_TEMP+jsonfile) as f:
                data = json.load(f)
            threshold=data['images'][0]['threshold']
            for categories in data['categories']:
                label_dict.update({categories['id']: categories['name']})
                color_dict.update({categories['id']: categories['color']})
        
            logger.info("\n label_dict " + str(label_dict))
            logger.info("\n color_dict " + str(color_dict))
            for annotation in data['annotations']:
                if annotation['probability'] >= threshold:
                    xmin = int(annotation['bbox'][0])
                    ymin = int(annotation['bbox'][1])
                    xmax = int(annotation['bbox'][0] + annotation['bbox'][2])
                    ymax = int(annotation['bbox'][1] + annotation['bbox'][3])
                    color = ImageColor.getrgb(color_dict.get(annotation['category_id']))
                    cv2.rectangle(output_image, (xmin, ymin), (xmax, ymax), color, 2)
                    text_size = cv2.getTextSize(label_dict.get(annotation['category_id']) + ' : %.2f' % annotation['probability'], cv2.FONT_HERSHEY_PLAIN, 1, 1)[0]
                    cv2.rectangle(output_image, (xmin, ymin), (xmin + text_size[0] + 3, ymin + text_size[1] + 4), color, -1)
                    cv2.putText(output_image, label_dict.get(annotation['category_id']) + ' : %.2f' % annotation['probability'],
                                (xmin, ymin + text_size[1] + 4), cv2.FONT_HERSHEY_PLAIN, 1, (255, 255, 255), 1)

            imagepred=image.strip('.'+imageextension)+suffix+'.'+imageextension
            cv2.imwrite(CONST_TEMP+imagepred, output_image)
            logger.info("path png "+str(path.exists(CONST_TEMP+imagepred)))
            logger.info("path png size "+str(path.getsize(CONST_TEMP+imagepred)))
            s3_client.upload_file(CONST_TEMP+imagepred,
                                  bucket,
                                  s3path+imagepred)
            url=s3_client.generate_presigned_url('get_object',
                                                            Params={'Bucket': bucket,
                                                                    'Key': s3path+imagepred},
                                                            ExpiresIn=3600)
            logger.info(url)
            os.remove(CONST_TEMP+imagepred)
            os.remove(CONST_TEMP+image)
            os.remove(CONST_TEMP+jsonFile)
            logger.info("path png image"+str(path.exists(CONST_TEMP+image)))
            logger.info("path png jsonfile"+str(path.exists(CONST_TEMP+jsonfile)))
            logger.info("path png imagepred"+str(path.exists(CONST_TEMP+imagepred)))
            if obj['DownloadRequired']==1:
                with conn.cursor() as cur:
                    query='update download_details set DownloadStatus=%s, DownloadURL=%s where DownloadRequestId=%s and FolderId=%s'
                    cur.execute(query,(3,url,obj['DownloadRequestId'],obj['FolderId']))
            return {
                "statusCode": 200,
                "headers": {
                    "Content-Type": "application/json"
                },
                "body": url
            }
        except Exception as e:
            with conn.cursor() as cur:
                    query='update download_details set DownloadStatus=%s where DownloadRequestId=%s and FolderId=%s'
                    cur.execute(query,(4,obj['DownloadRequestId'],obj['FolderId']))
            return {
                "statusCode": 200,
                "headers": {
                    "Content-Type": "application/json"
                },
                "body": str(type(e)) + ' ' + str(e)
            }
