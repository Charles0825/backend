const mqtt = require("mqtt");
const { Client } = require("pg");
const _ = require("lodash");
const cron = require("node-cron");
require("dotenv").config();

async function calculateAndSaveAverages(timeInterval, client) {
  try {
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
    // Get the most recent processed hour from the hourly_averages table
    const lastProcessedQuery = `
        SELECT MAX(timestamp) AS last_processed_hour
        FROM hourly_averages;
      `;
    const lastProcessedResult = await client.query(lastProcessedQuery);
    const lastProcessedHour = lastProcessedResult.rows[0].last_processed_hour;

    if (lastProcessedHour) {
      // Delete sensor data up to the most recent processed hour
      const deleteQuery = `
          DELETE FROM sensor_data
          WHERE timestamp < $1;
        `;
      await client.query(deleteQuery, [lastProcessedHour]);

      console.log(`Old data deleted up to ${lastProcessedHour}.`);
    } else {
      console.log("No processed data found to determine deletion range.");
    }
  } catch (err) {
    console.error("Error deleting old data", err.stack);
  }
}

async function resetPzem() {
  const mqttClient = mqtt.connect("mqtt://raspi:1883");

  try {
    mqttClient.publish("pzem/energy/reset", "RESET", { retain: false });
    console.log("Published RESET to topic pzem/energy/reset.");
  } catch (err) {
    console.error("Error resetting PZEM", err.stack);
  } finally {
    mqttClient.end();
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

    mqttClient.publish("pzem/energy/reset", "RESET", { retain: false });
    console.log("Published RESET to topic pzem/energy/reset.");

    await calculateAndSaveAverages("hour", client);

    await deleteOldData(client);
  } catch (err) {
    console.error("Error during the average calculation process:", err.stack);
  } finally {
    mqttClient.end();
    await client.end();
  }
}

// Schedule the PZEM reset at midnight (00:00) every day
cron.schedule("0 0 * * *", () => {
  resetPzem();
  console.log("PZEM reset triggered at midnight.");
});

module.exports = {
  calculateAndSaveAllAverages,
};
