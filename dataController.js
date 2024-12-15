const mqtt = require("mqtt");
const { Client } = require("pg");
const _ = require("lodash");
require("dotenv").config();

async function calculateAndSaveAverages(timeInterval, client) {
  try {
    // Define the SQL query based on the time interval (hourly)
    const avgQuery = `
        SELECT 
          date_trunc($1, timestamp) AS period,
          device_name,
          AVG(voltage) AS avg_voltage,
          AVG(current) AS avg_current,
          AVG(active_power) AS avg_active_power,
          MAX(energy) AS max_energy,
          AVG(frequency) AS avg_frequency,
          AVG(power_factor) AS avg_power_factor
        FROM sensor_data
        GROUP BY period, device_name
        ORDER BY period, device_name;
      `;

    const result = await client.query(avgQuery, [timeInterval]);

    // Define the insert query based on the table corresponding to the time interval
    let insertQuery;
    let tableName;
    if (timeInterval === "hour") {
      tableName = "hourly_averages";
    }

    insertQuery = `
        INSERT INTO ${tableName} (
          timestamp, device_name, avg_voltage, avg_current, avg_active_power,
          max_energy, avg_frequency, avg_power_factor
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
      `;

    // Step 2: Insert each period's average into the corresponding table
    for (const row of result.rows) {
      await client.query(insertQuery, [
        row.period,
        row.device_name,
        row.avg_voltage,
        row.avg_current,
        row.avg_active_power,
        row.max_energy,
        row.avg_frequency,
        row.avg_power_factor,
      ]);
    }

    console.log(
      `${
        timeInterval.charAt(0).toUpperCase() + timeInterval.slice(1)
      } averages calculated and saved successfully.`
    );
  } catch (err) {
    console.error(
      `Error calculating or saving ${timeInterval} averages`,
      err.stack
    );
  }
}

async function deleteOldData(client) {
  try {
    // Get the current date (excluding today's data)
    const currentDateQuery = `SELECT CURRENT_DATE`;
    const result = await client.query(currentDateQuery);
    const currentDate = result.rows[0].current_date;

    // Delete data older than the current date (excluding today's data)
    const deleteQuery = `
        DELETE FROM sensor_data
        WHERE timestamp < $1;
      `;

    // Execute the delete query
    await client.query(deleteQuery, [currentDate]);

    console.log("Old data deleted successfully, excluding today's data.");
  } catch (err) {
    console.error("Error deleting old data", err.stack);
  }
}

async function checkAndSaveCurrentDate(client) {
  try {
    // Get the current date (this is the date without time)
    const currentDateQuery = `SELECT CURRENT_DATE`;
    const result = await client.query(currentDateQuery);
    const currentDate = result.rows[0].current_date;

    // Check if today's date is already saved in the database
    const lastDateQuery = `
        SELECT "current_date"
        FROM date_tracker
        ORDER BY last_updated DESC
        LIMIT 1;
      `;
    const lastDateResult = await client.query(lastDateQuery);

    if (
      lastDateResult.rowCount > 0 &&
      _.isEqual(lastDateResult.rows[0].current_date, currentDate)
    ) {
      // The current date is already saved, so no need to continue processing
      console.log(
        "Today's date is already saved in the database. No need to process."
      );
      return false; // Indicating no need to proceed
    }

    // If the current date is not saved, insert it into the database
    const insertDateQuery = `
        INSERT INTO date_tracker ("current_date")
        VALUES ($1);
      `;
    await client.query(insertDateQuery, [currentDate]);

    console.log("Current date saved to database.");
    return true;
  } catch (err) {
    console.error("Error checking or saving current date", err.stack);
    return false;
  }
}

async function calculateAndSaveAllAverages() {
  const client = new Client({
    host: process.env.PG_DATABASE_HOST,
    port: 5432,
    user: "admin",
    password: "admin",
    database: "sensor_data",
  });

  const mqttClient = mqtt.connect("mqtt://raspi:1883");

  try {
    await client.connect();

    const shouldProceed = await checkAndSaveCurrentDate(client);

    if (shouldProceed) {
      mqttClient.publish("pzem/energy/reset", "RESET", { retain: false });
      console.log("Published RESET to topic pzem/energy/reset.");

      await calculateAndSaveAverages("hour", client);

      await deleteOldData(client);
    } else {
      console.log("No new data to process today.");
    }
  } catch (err) {
    console.error("Error during the average calculation process:", err.stack);
  } finally {
    mqttClient.end();
    await client.end();
  }
}

module.exports = {
  calculateAndSaveAllAverages,
};
