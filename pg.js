const { Client } = require("pg");
const crypto = require("crypto");
require("dotenv").config();

const client = new Client({
  host: process.env.PG_DATABASE_HOST,
  port: 5432, // default PostgreSQL port
  user: "admin", // your PostgreSQL username
  password: "admin", // your PostgreSQL password
  database: "sensor_data", // your database name
});
// Connect to the database
client.connect();

// Function to query device names from sensor_data
const getDeviceNames = async () => {
  const query = "SELECT DISTINCT device_name FROM hourly_averages;"; // Adjust the column name as per your schema

  try {
    const res = await client.query(query);
    return res.rows.map((row) => row.device_name); // Extract and return device names as an array
  } catch (error) {
    console.error(`Error querying device names: ${error.message}`);
    throw new Error(`Error querying device names: ${error.message}`);
  }
};

// Function to query all sensor data
const querySensorData = async () => {
  // await checkDatabase(); // Ensure the database exists
  const query = "SELECT * FROM sensor_data;"; // Assuming "sensordata" is your table name in TimescaleDB

  try {
    const res = await client.query(query);
    // console.log(res.rows);

    return res.rows; // Return the data as is
  } catch (error) {
    console.error(`Error querying TimescaleDB: ${error.message}`);
    throw new Error(`Error querying TimescaleDB: ${error.message}`);
  }
};

const querySensorDataHourlyMonthly = async () => {
  // await checkDatabase(); // Ensure the database exists
  const query = `
    SELECT device_name, DATE_TRUNC('month', timestamp) AS month, MAX(max_energy) AS highest_energy
    FROM hourly_averages
    GROUP BY device_name, month
    ORDER BY month DESC, device_name
  `; // Query to get the highest max_energy for each device on a monthly basis

  try {
    const res = await client.query(query);
    console.log(res.rows);

    return res.rows; // Return the rows with device_name, month, and highest energy values
  } catch (error) {
    console.error(`Error querying TimescaleDB: ${error.message}`);
    throw new Error(`Error querying TimescaleDB: ${error.message}`);
  }
};

// querySensorDataHourlyMonthly();

// filter it and get only the yesterday data for each device and add the energy values
const querySensorDataHourlyDaily = async () => {
  // await checkDatabase(); // Ensure the database exists
  const query = `
    SELECT device_name, DATE_TRUNC('day', timestamp) AS day, MAX(max_energy) AS highest_energy
    FROM hourly_averages
    GROUP BY device_name, day
    ORDER BY day DESC, device_name
  `; // Query to get the highest max_energy for each device on a daily basis

  try {
    const res = await client.query(query);
    console.log(res.rows);

    return res.rows; // Return the rows with device_name, day, and highest energy values
  } catch (error) {
    console.error(`Error querying TimescaleDB: ${error.message}`);
    throw new Error(`Error querying TimescaleDB: ${error.message}`);
  }
};

// querySensorDataHourlyDaily();
// get the max energy each device
const queryMaxEnergyToday = async () => {
  // Ensure the database exists
  const query = `
    SELECT device_name, DATE_TRUNC('day', timestamp) AS day, MAX(energy) AS max_energy
    FROM sensor_data
    WHERE DATE_TRUNC('day', timestamp) = CURRENT_DATE
    GROUP BY device_name, day
    ORDER BY device_name;
  `; // Query to get the max energy per device for today

  try {
    const res = await client.query(query);
    console.log(res.rows);

    return res.rows; // Return rows with device_name, today, and max energy values
  } catch (error) {
    console.error(`Error querying TimescaleDB: ${error.message}`);
    throw new Error(`Error querying TimescaleDB: ${error.message}`);
  }
};

// queryMaxEnergyToday();

// Function to query all sensor data
const querySensorDataHourly = async () => {
  // await checkDatabase(); // Ensure the database exists
  const query = "SELECT * FROM hourly_averages"; // Assuming "sensordata" is your table name in TimescaleDB

  try {
    const res = await client.query(query);
    //console.log(res.rows);

    return res.rows; // Return the data as is
  } catch (error) {
    console.error(`Error querying TimescaleDB: ${error.message}`);
    throw new Error(`Error querying TimescaleDB: ${error.message}`);
  }
};

// Function to delete sensor data based on id and device name
const deleteSensorData = async (id, deviceName) => {
  try {
    if (!id || !deviceName) {
      throw new Error("Missing id or deviceName for deletion.");
    }

    const query = `DELETE FROM hourly_averages WHERE "id" = $1 AND "device_name" = $2`;
    await client.query(query, [id, deviceName]);
  } catch (error) {
    console.error(
      `Error deleting sensor data from TimescaleDB: ${error.message}`
    );
    throw new Error(`Error deleting sensor data: ${error.message}`);
  }
};

// Function to fetch a user from TimescaleDB by username
const getUserFromDB = async (username) => {
  try {
    const result = await client.query(
      'SELECT * FROM "users" WHERE "username" = $1',
      [username]
    );
    if (result.rows.length === 0) return null;

    const user = result.rows[0];
    return {
      id: user.id,
      username: user.username,
      passwordHash: user.password,
    };
  } catch (error) {
    console.error(`Error fetching user from TimescaleDB: ${error.message}`);
    throw new Error(`Error fetching user: ${error.message}`);
  }
};

// Function to hash passwords (if needed)
const hashPassword = (password) => {
  return crypto.createHash("sha256").update(password).digest("hex");
};

// Function to change username or password
const changeUserDetail = async (username, newUsername, newPasswordHash) => {
  try {
    const user = await getUserFromDB(username);
    if (!user) {
      throw new Error("User not found");
    }

    // Step 1: Delete the existing user data
    await client.query('DELETE FROM "users" WHERE "id" = $1', [user.id]);

    // Step 2: Prepare the fields for updating
    const fieldsToUpdate = {
      username: newUsername || user.username, // Use new username or retain old one
      password: newPasswordHash
        ? hashPassword(newPasswordHash)
        : user.passwordHash, // Update password if provided
    };

    // Step 3: Insert the new data point
    await client.query(
      `INSERT INTO "users" (id, username, password) VALUES ($1, $2, $3)`,
      [user.id, fieldsToUpdate.username, fieldsToUpdate.password]
    );

    const updateType = newUsername ? "Username" : "Password";
    return { message: `${updateType} updated successfully` };
  } catch (error) {
    console.error(`Error changing user detail: ${error.message}`);
    throw new Error(`Error changing user detail: ${error.message}`);
  }
};

// Export functions
module.exports = {
  querySensorData,
  deleteSensorData,
  getUserFromDB,
  changeUserDetail,
  querySensorDataHourly,
  getDeviceNames,
  queryMaxEnergyToday,
  querySensorDataHourlyDaily,
  querySensorDataHourlyMonthly,
};
