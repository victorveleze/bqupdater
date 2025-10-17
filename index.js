// Import necessary libraries
const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');

// Initialize Express and BigQuery client
const app = express();
const bigquery = new BigQuery();

// Define your BigQuery dataset and table
const datasetId = 'Servinet'; // e.g., 'my_dataset'
const tableId = 'client_daily_state';     // e.g., 'client_activity'

app.post('/', async (req, res) => {
  try {
    // Data to be inserted. This could come from the request body (req.body)
    // For this example, we'll use static data.
    const newRow = {
      client_id: 12345,
      fecha: new Date().toISOString().slice(0, 10), // "YYYY-MM-DD"
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

  let errorMessage = 'Failed to insert data.';
  
  // Include BigQuery insert errors if present
  if (error.name === 'PartialFailureError') {
    errorMessage += ' ' + JSON.stringify(error.errors);
  } else {
    errorMessage += ' ' + error.message;
  }

  res.status(500).send(errorMessage);
  }
});

app.get('/', (req, res) => {
  res.send('Hello from Victor Cloud Run!');
});

// Cloud Run provides PORT via env var
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Server listening on port ${port}`);
});