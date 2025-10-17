// Import necessary libraries
const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');

// Initialize Express and BigQuery client
const app = express();
const bigquery = new BigQuery();

// Define your BigQuery dataset and table
const datasetId = 'Servinet'; // e.g., 'my_dataset'
const tableId = 'ClientDailyState';     // e.g., 'client_activity'

async function upsert(row) {
const query = `
  MERGE INTO \`${datasetId}.${tableId}\` T
  USING (
    SELECT
      @client_id AS client_id,
      CAST(@date AS DATE) AS date,
      @state AS state
  ) S
  ON T.client_id = S.client_id AND T.date = S.date
  WHEN MATCHED THEN
    UPDATE SET T.state = S.state
  WHEN NOT MATCHED THEN
    INSERT (client_id, date, state)
    VALUES(S.client_id, S.date, S.state);
`;


  const options = {
    query: query,
    params: {
      client_id: row.client_id,
      date: row.date,      // Make sure this is in 'YYYY-MM-DD' format or a JS Date object
      state: row.state
    }
  };

  const [job] = await bigquery.createQueryJob(options);
  await job.getQueryResults();
  console.log(`Upsert completed for client_id: ${row.client_id}, date: ${row.date}`);
}

app.post('/', async (req, res) => {
  try {
    // Data to be inserted. This could come from the request body (req.body)
    // For this example, we'll use static data.
    const newRow = {
      client_id: "12345",
      date: new Date().toISOString().slice(0, 10), // "YYYY-MM-DD"
      state: 'cancelado'
    };

    // Insert the row into the BigQuery table
    await upsert(newRow);

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