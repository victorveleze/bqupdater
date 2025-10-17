// Import necessary libraries
const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');

// Initialize Express and BigQuery client
const app = express();
const bigquery = new BigQuery();

// Define your BigQuery dataset and table
const datasetId = 'YOUR_DATASET_ID'; // e.g., 'my_dataset'
const tableId = 'YOUR_TABLE_ID';     // e.g., 'client_activity'

app.post('/', async (req, res) => {
  try {
    // Data to be inserted. This could come from the request body (req.body)
    // For this example, we'll use static data.
    const newRow = {
      client_id: 12345,
      fecha: new Date().toISOString(), // Use ISO string for TIMESTAMP or DATETIME
      estado: 'activo'
    };

    // Insert the row into the BigQuery table
    await bigquery
      .dataset(datasetId)
      .table(tableId)
      .insert([newRow]); // The insert method expects an array of objects

    console.log(`Inserted 1 row into ${datasetId}.${tableId}`);
    res.status(200).send('Data added to BigQuery successfully!');

  } catch (error) {
    console.error('ERROR:', error);
    // Check for insert errors
    if (error.name === 'PartialFailureError') {
      console.error('Insert Errors:', error.errors);
    }
    res.status(500).send('Failed to insert data.');
  }
});

// Start the server
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});