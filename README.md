- The program downloads the pictures from the website(myauto) and sends them to the AI recognition software(carnet.ai).
- The result is then stored in dynamoDB table.
- In case of an error, the error is sent to AWS Rekognition and the response is stored in rekogintionAnalysesDB.
