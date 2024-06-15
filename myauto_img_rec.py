import asyncio
import aiohttp
import os
import zipfile
import boto3
from botocore.exceptions import NoCredentialsError


auto_page_n = lambda nth_page: f"https://api2.myauto.ge/ka/products?TypeID=0&ForRent=&Mans=&CurrencyID=3&MileageType=1&Page={nth_page}"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/58.0.3029.110 Safari/537.3"
}

async def download_image(session, url, save_directory):
    try:
        async with session.get(url) as response:
            response.raise_for_status()

            filename = os.path.basename(url)
            save_path = os.path.join(save_directory, filename)
            with open(save_path, 'wb') as file:
                while True:
                    chunk = await response.content.read(1024)
                    if not chunk:
                        break
                    file.write(chunk)

            print(f"Downloaded: {filename}")
    except aiohttp.ClientError as e:
        print(f"Error downloading image: {e}")

def upload_to_s3(file_path, bucket_name, s3_client):
    try:
        filename = os.path.basename(file_path)
        s3_client.upload_file(file_path, bucket_name, filename)
        print(f"Uploaded {filename} to {bucket_name}")
    except NoCredentialsError:
        print("Credentials not available")
    except Exception as e:
        print(f"An error occurred: {e}")

async def main():
    bucket_name = 'my-auto-rec-bucket'
    image_urls = []

    async with aiohttp.ClientSession(headers=headers) as session:
        for page_n in range(1):
            response = await session.get(auto_page_n(page_n))
            response.raise_for_status()

            data = await response.json()

            for item in data['data']['items']:
                car_id = item['car_id']
                photo = item['photo']
                picn = item['pic_number']
                print(f"Car ID: {car_id}")
                print("Image URLs:")
                for id in range(1, picn + 1):
                    image_url = f"https://static.my.ge/myauto/photos/{photo}/large/{car_id}_{id}.jpg"
                    image_urls.append(image_url)
                    print(image_url)
                print()

        save_directory = "downloaded_images"
        os.makedirs(save_directory, exist_ok=True)

        tasks = []
        async with aiohttp.ClientSession() as session:
            for url in image_urls:
                task = asyncio.ensure_future(download_image(session, url, save_directory))
                tasks.append(task)

            await asyncio.gather(*tasks)

        zip_filename = "downloaded_images.zip"
        with zipfile.ZipFile(zip_filename, 'w') as zip_file:
            for root, _, files in os.walk(save_directory):
                for file in files:
                    file_path = os.path.join(root, file)
                    zip_file.write(file_path, arcname=file)

        print(f"\nAll images downloaded and zipped successfully.")
        zip_file_size_mb = os.path.getsize(zip_filename) / (1024 * 1024)
        print(f"ZIP file size: {zip_file_size_mb:.2f} MB")

        total_images = sum(len(files) for _, _, files in os.walk(save_directory))
        print(f"Total number of downloaded images: {total_images}")

        # Upload to S3
        s3_client = boto3.client('s3')
        for root, _, files in os.walk(save_directory):
            for file in files:
                file_path = os.path.join(root, file)
                upload_to_s3(file_path, bucket_name, s3_client)
        upload_to_s3(zip_filename, bucket_name, s3_client)

if __name__ == "__main__":
    asyncio.run(main())



################
##################
########################
# Lambda Funciton
# ეს ფუნქცია ჩავსვი aws lambda-ში და ტრიგერად მივუთითე s3 ბაკეტი(მასზე ნებისმიერი ობიექტის ატვირთვა)
import json
import boto3
import requests
import time
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
rekognition = boto3.client('rekognition')

carnet_table = dynamodb.Table('carnetResponseDB')
rekognition_table = dynamodb.Table('rekognitionAnalysesDB')

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        s3_url = f'https://{bucket}.s3.amazonaws.com/{key}'

        try:
            carnet_response = send_image_to_carnet(s3_url)
            if carnet_response:
                save_to_dynamodb(carnet_table, key, carnet_response)
                if carnet_response.get('error') == "Image doesn't contain a car":
                    rekognition_response = analyze_with_rekognition(bucket, key)
                    save_to_dynamodb(rekognition_table, key, rekognition_response)
        except Exception as e:
            print(f"Error processing image {key}: {e}")

def send_image_to_carnet(image_url):
    while True:
        response = requests.post('https://carnet.ai/recognize-url', data={'url': image_url})
        status_code = response.status_code

        if status_code == 200:
            return response.json()
        elif status_code == 429:
            print("API rate limit exceeded: 429. Retrying after half a second...")
            time.sleep(0.5)
        elif status_code == 500:
            print(f"Server error: {status_code}")
            return response.json()
        else:
            print(f"Unexpected API response: {status_code}")
            return None

def analyze_with_rekognition(bucket, key):
    try:
        response = rekognition.detect_labels(
            Image={'S3Object': {'Bucket': bucket, 'Name': key}},
            MaxLabels=10,
            MinConfidence=75
        )
        return response
    except Exception as e:
        print(f"Error analyzing image with Rekognition: {e}")
        return None

def save_to_dynamodb(table, key, data):
    try:
        table.put_item(
            Item={
                'imageKey': key,
                'response': data
            }
        )
        print(f"Data saved to DynamoDB for key: {key}")
    except Exception as e:
        print(f"Error saving to DynamoDB: {e}")

