# Endpoint Refactoring

### Endpoint: **`Md5FromAA`**

- The function serves as an endpoint for processing requests from Acuity.
- It checks for MD5 hashed email in the query parameters, validates the required fields, and generates responses based on the validation results.
- The function incorporates logging statements to record information about the requests, eliminating the need for storage.

```jsx
/**
 * Cloud Function to handle responses from Acuity.
 * @param {Object} req - Cloud Function request context.
 * @param {Object} res - Cloud Function response context.
 */
exports.Md5FromAA = async (req, res) => {
    try {
        console.info("Md5FromAA: Request received.");

        // Extract MD5 hashed email from query parameters
        const md5HashedEmail = req.query.md5;

        // Validate MD5 hashed email exists
        if (!md5HashedEmail) {
            console.error("Md5FromAA: MD5 hashed email not provided in the query parameters.");
            return res.status(400).json({ error: 'MD5 hashed email not provided in the query parameters.' });
        }

        // Extract additional query parameters
        const queryParameters = {
            locationId: req.query.locationId || '',
            formId: req.query.formId || '',
        };

        // Check if locationId is present
        if (!queryParameters.locationId) {
            console.error("Md5FromAA: Missing 'locationId' parameter.");
            return res.status(400).json({ error: "Missing 'locationId' parameter." });
        }

        // Send the response
        return res.status(200).json({
            success: true,
            message: "Request processed successfully.",
        });
    } catch (error) {
        console.error("Md5FromAA: Error during processing.", error);

        // Return internal server error response
        return res.status(500).json({
            error: "Internal server error. Please check server logs for details.",
        });
    }
};
```

### **response-batch-processing**

- Scheduled cron job that triggers **batchResponseData** at 7:00 AM UTC-6 (CST) every day

**`batchResponseData`**

- The Node script is tasked with querying GCP logs to extract entries timestamped from 7:00 AM the day before to 7:00 AM on the current day.
- It automates the extraction and storage of MD5, locationId, and formId parameters from Cloud Logs.
- The script organizes data storage for future processing.
- It dynamically stores and names the file, such as **`bulklog_${current_date}.json`**.

```jsx
// Import required modules
const { v4: uuidv4 } = require('uuid');
const { Logging } = require('@google-cloud/logging');
const { Storage } = require('@google-cloud/storage');

// Initialize Google Cloud Logging and Storage clients
const logging = new Logging();
const storage = new Storage();

// Track total and unique md5 counts per locationId
const locationStats = {};

/**
 * Extracts values from log data.
 * @param {Array} data - Log data.
 * @returns {Array} - Extracted values.
 */
function extractValues(data) {
    const result = [];

    for (const item of data) {
        if (item instanceof Object && 'insertId' in item && 'httpRequest' in item) {
            const http_request = item['httpRequest'];
            if ('requestUrl' in http_request) {
                // Extract values from the request URL
                const url = new URL(http_request['requestUrl']);
                const md5 = url.searchParams.get('md5');
                const formId = url.searchParams.get('formId');
                const locationId = url.searchParams.get('locationId');

                // Update locationStats
                updateLocationStats(locationId, md5);

                // Add extracted values to result
                result.push({
                    'insertId': item['insertId'],
                    'md5': md5,
                    'formId': formId,
                    'locationId': locationId
                });
            }
        } else if (Array.isArray(item)) {
            result.push(...extractValues(item));
        } else if (item instanceof Object) {
            result.push(...extractValues(Object.values(item)));
        }
    }

    return result;
}

/**
 * Updates locationStats based on the extracted values.
 * @param {string} locationId - Location identifier.
 * @param {string} md5 - MD5 value.
 */
function updateLocationStats(locationId, md5) {
    if (!locationStats[locationId]) {
        locationStats[locationId] = { total: 0, unique: 0 };
    }
    locationStats[locationId].total++;
    if (!result.some(entry => entry.locationId === locationId && entry.md5 === md5)) {
        locationStats[locationId].unique++;
    }
}

/**
 * Groups extracted values by location.
 * @param {Array} values - Extracted values.
 * @returns {Array} - Grouped values.
 */
function groupValuesByLocation(values) {
    const groupedValues = {};

    values.forEach(item => {
        const { locationId, md5 } = item;

        if (!groupedValues[locationId]) {
            groupedValues[locationId] = {
                locationId: locationId,
                objectList: new Set(), // Use Set to ensure uniqueness
            };
        }

        groupedValues[locationId].objectList.add(md5); // Add to Set to ensure uniqueness
    });

    // Convert Set to array before returning
    return Object.values(groupedValues).map(entry => ({
        locationId: entry.locationId,
        objectList: [...entry.objectList],
    }));
}

/**
 * Stores grouped values in Google Cloud Storage.
 * @param {Array} chunkedValues - Grouped and chunked values.
 */
async function storeValuesInStorage(chunkedValues) {
    const bucketName = 'fuel-response-data';
    const currentDate = new Date().toISOString().split('T')[0];
    const bucket = storage.bucket(bucketName);

    const flattenedChunks = [];

    chunkedValues.forEach(group => {
        const { locationId, objectList } = group;

        // Split objectList into chunks of max 100 MD5 values
        for (let i = 0; i < objectList.length; i += 100) {
            // Generate unique requestId for each chunk
            const requestId = uuidv4().replace(/-/g, '');
            const chunk = objectList.slice(i, i + 100);

            flattenedChunks.push({
                locationId: locationId,
                requestId: requestId,
                objectList: chunk,
            });
        }
    });

    const jsonString = JSON.stringify(flattenedChunks, null, 2);

    const fileName = `bulklog_${currentDate}.json`;
    const file = bucket.file(fileName);
    
    // Log locationStats before storing the file
    console.log(JSON.stringify(locationStats, null, 2));

    await file.save(jsonString, {
        metadata: {
            contentType: 'application/json',
        },
    });

    console.log(`Values stored in Cloud Storage: ${fileName}`);
}

/**
 * Cloud Function to batch process response data from logs.
 * @param {Object} req - Cloud Function request context.
 * @param {Object} res - Cloud Function response context.
 */
exports.batchResponseData = async (req, res) => {
    try {
        console.info("Extracting values from Cloud Logs...");

        // Define log query parameters
        const logQuery = `
            (resource.type="cloud_function"
            AND resource.labels.function_name="Md5FromAA"
            AND resource.labels.region="us-central1"
            AND httpRequest.requestMethod="GET"
            AND httpRequest.status=200)
            OR
            (resource.type="cloud_run_revision"
            AND resource.labels.service_name="md5identitiesfromaa"
            AND resource.labels.location="us-central1"
            AND httpRequest.requestMethod="GET"
            AND httpRequest.status=200)
            severity>=DEFAULT`;

        // Set start and end times for log retrieval
        const startTime = new Date();
        startTime.setHours(7, 0, 0, 0);
        startTime.setDate(startTime.getDate() - 1);

        const endTime = new Date();
        endTime.setHours(7, 0, 0, 0);

        // Retrieve logs based on the defined parameters
        const logsResponse = await logging.getEntries({
            filter: logQuery,
            orderBy: 'timestamp',
            pageSize: 1000,
            startTime: startTime.toISOString(),
            endTime: endTime.toISOString(),
        });

        const logs = logsResponse[2].entries;
        const extractedValues = extractValues(logs);
        const groupedValues = groupValuesByLocation(extractedValues);

        // Store grouped values in Google Cloud Storage
        await storeValuesInStorage(groupedValues);

        console.info("Value extraction and storage complete.");

        res.status(200).send("Value extraction and storage complete.");
    } catch (error) {
        console.error("Error during value extraction and storage.", error);
        res.status(500).send("Error during value extraction and storage.");
    }
};

/**
 * Generates a random UUID.
 * @returns {string} - Random UUID.
 */
function generateRandomId() {
    return uuidv4().replace(/-/g, '');
}
```

### **scheduled-batch-request**

- Scheduled cron job that triggers **`scheduledBatchRequest`** cloud function.
- The Node script is designed for scheduled batch requests to Acuity for full identities.

### scheduledBatchRequest

- A serverless node script that dynamically locates and retrieves data from the **`bulklog_${current_date}.json`** file generated by the **`batchResponseData`** function
- It aggregates responses based on location IDs:
    - Organizing and analyzing API responses corresponding to their respective locations.
    - Creating a file for each location ID, storing multiple API responses based on locationId.

```jsx
// Import required modules
const { Storage } = require('@google-cloud/storage');
const axios = require('axios');

// Define constant values for bucket names and API URLs
const INPUT_BUCKET_NAME = 'response-data';
const OUTPUT_BUCKET_NAME = 'full-identity';

const TOKEN_URL = 'https://aws-api.com/oauth2/token';
const MD5_API_URL = 'https://aws-api.com/email';

// Initialize Google Cloud Storage
const storage = new Storage();

const { CLIENT_ID, CLIENT_SECRET } = process.env;

// Define locationResponses object to store aggregated API responses
const locationResponses = {};

/**
 * Scheduled batch request function to process pre-structured data.
 */
exports.scheduledBatchRequest = async () => {
    try {
        // Log the start of the scheduled job
        console.log('Scheduled job started.');

        const currentDate = new Date().toISOString().split('T')[0];
        const inputFilePath = `bulklog_${currentDate}.json`; // Updated file path

        // Read pre-structured data from the specified file path
        const preStructuredData = await readPreStructuredData(inputFilePath);

        if (!preStructuredData || preStructuredData.length === 0) {
            console.error('No pre-structured data found.');
            return;
        }

        // Request an access token for API calls
        const accessToken = await requestAccessToken();

        // Process each data entry in the pre-structured data
        for (const dataEntry of preStructuredData) {
            const { locationId, requestId, objectList } = dataEntry;

            if (!locationId || !requestId || !objectList || !Array.isArray(objectList)) {
                console.error('Invalid data entry:', dataEntry);
                continue;
            }

            // Process data entry and make API calls
            await processDataEntry(locationId, requestId, objectList, accessToken);
            console.log(`Processed data entry with LocationId: ${locationId}, RequestId: ${requestId}`);
        }

        // Save aggregated responses to Google Cloud Storage
        await saveAggregatedResponses();

        // Log the successful completion of the scheduled job
        console.log('Scheduled job completed successfully.');
    } catch (error) {
        // Log any errors that occur during the job
        console.error(`Error processing scheduled job. Message: ${error.message}`);
    }
};

/**
 * Read pre-structured data from Google Cloud Storage.
 * @param {string} filePath - Path to the file containing pre-structured data.
 * @returns {Array} - Parsed pre-structured data.
 */
async function readPreStructuredData(filePath) {
    try {
        const bucket = storage.bucket(INPUT_BUCKET_NAME);
        const file = bucket.file(filePath);

        // Download and parse file content
        const [fileContent] = await file.download();
        const preStructuredData = JSON.parse(fileContent.toString());

        // Log successful reading of pre-structured data
        console.log('Pre-structured data read successfully.');

        return preStructuredData;
    } catch (error) {
        // Log errors and throw them for better error handling
        console.error(`Error reading pre-structured data. Message: ${error.message}`);
        throw error;
    }
}

/**
 * Process a data entry by making API calls and collecting responses.
 * @param {string} locationId - Location identifier.
 * @param {string} requestId - Request identifier.
 * @param {Array} objectList - List of objects to process.
 * @param {string} accessToken - Access token for API calls.
 */
async function processDataEntry(locationId, requestId, objectList, accessToken) {
    try {
        // Log the processing of a data entry
        console.log(`Processing data entry with LocationId: ${locationId}, RequestId: ${requestId}, ObjectList: ${objectList.join(', ')}`);

        // Make API call with the access token
        const apiResponse = await callApiWithAccessToken(accessToken, locationId, requestId, objectList);

        // Log the API response
        console.log('API Response:', apiResponse);

        // Collect the API response
        collectApiResponse(locationId, apiResponse);

        // Log the successful processing of a data entry
        console.log(`Processed data entry with LocationId: ${locationId}, RequestId: ${requestId}`);
    } catch (error) {
        // Log errors during data entry processing
        console.error(`Error processing data entry. Message: ${error.message}`);
    }
}

/**
 * Save aggregated responses to Google Cloud Storage.
 */
async function saveAggregatedResponses() {
    try {
        // Iterate through locationResponses and store aggregated responses
        for (const [locationId, aggregatedResponse] of Object.entries(locationResponses)) {
            await storeAggregatedApiResponse(locationId, aggregatedResponse);
        }

        // Log the successful saving of aggregated responses
        console.log('Aggregated responses saved successfully.');
    } catch (error) {
        // Log errors during saving aggregated responses
        console.error(`Error saving aggregated responses. Message: ${error.message}`);
    }
}

/**
 * Store aggregated API response for a specific location to Google Cloud Storage.
 * @param {string} locationId - Location identifier.
 * @param {Array} aggregatedResponse - Aggregated API responses.
 */
async function storeAggregatedApiResponse(locationId, aggregatedResponse) {
    const currentDate = new Date().toISOString().split('T')[0];
    const outputFileName = `aggregated_response_${locationId}_${currentDate}.json`;

    try {
        const outputBucket = storage.bucket(OUTPUT_BUCKET_NAME);
        const outputFile = outputBucket.file(outputFileName);

        // Save aggregated response to the specified file
        await outputFile.save(JSON.stringify(aggregatedResponse));

        // Log the successful storage of aggregated responses
        console.log(`Aggregated responses stored for LocationId: ${locationId}`);
    } catch (error) {
        // Log errors during storing aggregated responses
        console.error(`Error storing aggregated responses. LocationId: ${locationId}. Message: ${error.message}`);
        throw error;
    }
}

/**
 * Request access token from the authentication server.
 * @returns {string} - Access token for API calls.
 */
async function requestAccessToken() {
    try {
        // Make a POST request to obtain an access token
        const tokenResponse = await axios.post(
            TOKEN_URL,
            new URLSearchParams({
                grant_type: 'client_credentials',
                client_id: CLIENT_ID,
                client_secret: CLIENT_SECRET,
                scope: 'GetDataBy/Md5Email',
            }),
            {
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                },
            }
        );

        // Extract and return the access token from the response
        const accessToken = tokenResponse.data.access_token;
        
        // Log the successful access token request
        console.log('Access token requested successfully.');

        return accessToken;
    } catch (error) {
        // Log errors during access token request
        console.error(`Error requesting access token. ${error.response ? `Status: ${error.response.status}.` : ''} Message: ${error.message}`);
        throw new Error('Error requesting access token');
    }
}

/**
 * Make API call using the provided access token.
 * @param {string} accessToken - Access token for API calls.
 * @param {string} locationId - Location identifier.
 * @param {string} requestId - Request identifier.
 * @param {Array} objectList - List of objects to process.
 * @returns {Object} - API response.
 */
async function callApiWithAccessToken(accessToken, locationId, requestId, objectList) {
    const apiEndpoint = MD5_API_URL;

    try {
        // Make a POST request to the API endpoint with the provided access token
        const apiResponse = await axios.post(
            apiEndpoint,
            {
                RequestId: requestId,
                ObjectList: objectList,
                OutputId: 19,
            },
            {
                headers: {
                    Authorization: `Bearer ${accessToken}`,
                    'Content-Type': 'application/json',
                },
            }
        );

        // Return the API response data
        return apiResponse.data;
    } catch (error) {
        // Log errors during API call
        console.error(`Error making API call. Message: ${error.message}`);
        throw error;
    }
}

/**
 * Collect API response for a specific location.
 * @param {string} locationId - Location identifier.
 * @param {Object} apiResponse - API response.
 */
function collectApiResponse(locationId, apiResponse) {
    // Check if locationId exists in locationResponses, if not, initialize it as an empty array
    if (!locationResponses[locationId]) {
        locationResponses[locationId] = [];
    }

    // Store the API response along with locationId
    locationResponses[locationId].push({
        locationId: locationId,
        apiResponse: apiResponse
    });

    // Log the collected API response
    console.log(`Collected API response for LocationId: ${locationId}`);
}
```

### json_to_csv

- A Python Cloud Function Storage Trigger that fires on new file creation or updates in the **`full-identity`** Bucket.
- Processes JSON data by creating a Pandas DataFrame to store as CSV, adhering to the naming convention **`structured_{location_id}_{current_date}.csv`**

```jsx
import functions_framework
import json
import pandas as pd
import logging
from google.cloud import storage
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)

@functions_framework.cloud_event
def json_to_csv(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    time_created = data["timeCreated"]
    updated = data["updated"]

    logging.info(f"Received Cloud Storage event - Event ID: {event_id}, Event type: {event_type}")
    logging.info(f"Bucket: {bucket}, File: {name}, Metageneration: {metageneration}")
    logging.info(f"Created: {time_created}, Updated: {updated}")

    # Execute the logic in json_to_csv function
    process_json_to_csv({"bucket": bucket, "name": name})

def process_json_to_csv(event):
    # Extract the bucket and file name from the Cloud Storage event
    bucket_name = event['bucket']
    file_name = event['name']

    # Initialize a Google Cloud Storage client
    storage_client = storage.Client()

    # Get the JSON data from the Cloud Storage file
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    try:
        data = json.loads(blob.download_as_text())
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON data: {e}")
        return

    # Extract locationId
    location_id = data[0].get('locationId', 'unknown_location')

    logging.info(f"Processing JSON data for locationId: {location_id}")

    # Create a list to store processed data
    processed_data = []

    # Iterate through returnData
    for key, value in data[0].get('apiResponse', {}).get('returnData', {}).items():
        # Use the already hashed key as a unique identifier
        md5_hash = key

        # Create a dictionary to store processed object
        processed_object = {'md5': md5_hash}

        # Dynamically map array within nested objects to set the key as a column
        for item in value:
            processed_object.update(item)

        # Append the processed object to the list
        processed_data.append(processed_object)

    # Convert the list to a pandas DataFrame
    df = pd.DataFrame(processed_data)

    # Save the DataFrame to a CSV file using the updated naming convention
    current_date = datetime.now().strftime("%Y-%m-%d")
    csv_file_name = f"structured_{location_id}_{current_date}.csv"
    output_bucket_name = "fuel-final-files"
    output_bucket = storage_client.bucket(output_bucket_name)
    csv_blob = output_bucket.blob(csv_file_name)
    csv_blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')

    logging.info(f"CSV file '{csv_file_name}' created and uploaded successfully to '{output_bucket_name}' bucket.")
```
