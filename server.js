const express = require("express");
const cors = require("cors");
const dayjs = require("dayjs");
const _ = require("lodash");
const rateLimit = require("express-rate-limit");
const isBetween = require("dayjs/plugin/isBetween");
const cron = require("node-cron");
require("dotenv").config();
const utc = require("dayjs/plugin/utc");
const timezone = require("dayjs/plugin/timezone");

dayjs.extend(utc);
dayjs.extend(timezone);

const {
  querySensorData,
  deleteSensorData,
  getUserFromDB,
  changeUserDetail,
  querySensorDataHourly,
  getDeviceNames,
  queryMaxEnergyToday,
  querySensorDataHourlyDaily,
  querySensorDataHourlyMonthly,
} = require("./pg.js");

const { calculateAndSaveAllAverages } = require("./dataController.js");
const app = express();
const port = process.env.SERVER_PORT || 4444;

// Create a rate limiter: maximum of 10 requests per minute
const limiter = rateLimit({
  windowMs: 1 * 60 * 1000, // 1 minute
  max: 1000,
  message: "Too many requests from this IP, please try again later.",
});
// Use the CORS middleware
app.use(cors()); // Enable CORS for all routes
app.use(express.json());
// Apply the rate limiter to all requests
app.use(limiter);
dayjs.extend(isBetween);
// Root route
app.get("/", (req, res) => {
  res.send("Hello, World!");
});

app.get("/api/user/:username", async (req, res) => {
  const { username } = req.params;

  try {
    const user = await getUserFromDB(username);
    if (!user) {
      return res.status(404).json({ error: "User not found" });
    }

    res.json(user);
  } catch (error) {
    res.status(500).json({ error: "Error fetching user data" });
  }
});

app.put("/api/user/change", async (req, res) => {
  const { username, newUsername, newPasswordHash } = req.body;

  try {
    const response = await changeUserDetail(
      username,
      newUsername,
      newPasswordHash
    );
    res.status(200).json(response);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Route to handle the request
app.get("/api/devices", async (req, res) => {
  try {
    const deviceNames = await getDeviceNames();
    res.json(deviceNames); // Send device names as JSON response
  } catch (error) {
    console.error(`Error in /devices route: ${error.message}`);
    res.status(500).json({ error: "Failed to fetch device names" });
  }
});

app.get("/api/sensors", limiter, async (req, res) => {
  try {
    const { period, deviceName, startDate, endDate, singleDate } = req.query; // Get optional filters from query
    const rawData = await querySensorDataHourly(); // Fetch raw data from InfluxDB

    // Format the data with custom timestamp
    let formattedData = rawData.map((item) => ({
      timestamp: dayjs(item.timestamp).format("MM/DD/YYYY, hh:mm:ss A"),
      id: item["id"],
      deviceName: item["device_name"],
      voltage: parseFloat(item["avg_voltage"]) || 0, // Convert to float, default to 0 if invalid
      current: parseFloat(item["avg_current"]) || 0, // Convert to float, default to 0 if invalid
      activePower: parseFloat(item["avg_active_power"]) || 0, // Convert to float, default to 0 if invalid
      energy: parseFloat(item["max_energy"]) || 0, // Convert to float, default to 0 if invalid
      frequency: parseFloat(item["avg_frequency"]) || 0, // Convert to float, default to 0 if invalid
      powerFactor: parseFloat(item["avg_power_factor"]) || 0, // Convert to float, default to 0 if invalid
    }));

    // Filter by deviceName if specified
    if (deviceName) {
      formattedData = formattedData.filter(
        (item) => item.deviceName === deviceName
      );
    }

    // Filter by date range if specified
    // Filter by date range and specific day if specified
    if (startDate || endDate || singleDate) {
      const start = startDate ? dayjs(startDate) : null;
      const end = endDate ? dayjs(endDate) : null;
      const day = singleDate ? dayjs(singleDate, "YYYY-MM-DD") : null;

      formattedData = formattedData.filter((item) => {
        const itemDate = dayjs(item.timestamp, "MM/DD/YYYY, hh:mm:ss A");

        let isValid = true;

        // Apply date range filter
        if (start) {
          isValid = isValid && itemDate.isAfter(start, "day");
        }
        if (end) {
          isValid = isValid && itemDate.isBefore(end, "day");
        }

        if (day) {
          isValid = isValid && itemDate.isSame(day, "day");
        }

        return isValid;
      });
    }

    // If a period is specified, aggregate the data
    if (period) {
      formattedData = aggregateData(formattedData, period);
    }

    res.json({ data: formattedData });
  } catch (error) {
    res.status(500).json({ error: "Failed to retrieve sensor data." });
  }
});

function aggregateData(data, period) {
  // Group the data based on the specified period (hour, day, or month)
  const groupedData = _.groupBy(data, (item) => {
    const date = dayjs(item.timestamp, "MM/DD/YYYY, hh:mm:ss A");
    switch (period) {
      case "day":
        return date.format("MM/DD/YYYY");
      case "month":
        return date.format("MM/YYYY");
      default:
        throw new Error("Invalid period specified. Use 'day', or 'month'.");
    }
  });

  return Object.keys(groupedData).map((key) => {
    const group = groupedData[key];

    // Initialize accumulator variables for all fields
    let totalVoltage = 0;
    let totalCurrent = 0;
    let totalActivePower = 0;
    let totalFrequency = 0;
    let totalPowerFactor = 0;
    let maxEnergy = -Infinity; // Start with a low value for max energy

    // Collect data for the group in one pass
    const ids = [];
    let deviceName = group[0].deviceName; // Assuming deviceName is consistent
    group.forEach((item) => {
      totalVoltage += item.voltage || 0;
      totalCurrent += item.current || 0;
      totalActivePower += item.activePower || 0;
      totalFrequency += item.frequency || 0;
      totalPowerFactor += item.powerFactor || 0;
      maxEnergy = Math.max(maxEnergy, item.energy || 0); // Find max energy in the group
      ids.push(item.id); // Collect all IDs
    });

    // Calculate averages and return the aggregated result
    const groupSize = group.length;
    return {
      timestamp: key,
      id: ids, // Collect all IDs in an array
      deviceName: deviceName, // Assuming consistent device name
      voltage: _.round(totalVoltage / groupSize, 2),
      current: _.round(totalCurrent / groupSize, 2),
      activePower: _.round(totalActivePower / groupSize, 2),
      energy: maxEnergy, // Use the highest energy value
      frequency: _.round(totalFrequency / groupSize, 2),
      powerFactor: _.round(totalPowerFactor / groupSize, 2),
    };
  });
}

// app.get("/api/energy-usage-summary", limiter, async (req, res) => {
//   try {
//     console.log("Requesting......");
//     const rawData = await querySensorData();
//     const today = dayjs();
//     const startOfToday = today.startOf("day");
//     const startOfYesterday = today.subtract(1, "day").startOf("day");
//     const startOfThisMonth = today.startOf("month");
//     const startOf31DaysAgo = today.subtract(30, "days").startOf("day");
//     const endOfToday = today.endOf("day");

//     // Initialize variables for consumption summaries
//     let todaysConsumption = 0;
//     let yesterdaysConsumption = 0;
//     let thisMonthsConsumption = 0;
//     const runningDevices = new Set();
//     const dailyUsage = new Array(31).fill(0); // For daily usage over the last 31 days
//     const monthlyUsage = new Array(12).fill(0); // For monthly usage
//     const dailyEnergyUsage = {}; // For daily usage per device

//     // Process each data point in rawData
//     rawData.forEach((item) => {
//       const itemTime = dayjs(item.time);
//       const deviceName = item["device_name"];
//       const energy = item["energy"]; // Assuming energy is in kWh

//       // Daily, monthly, and running devices calculations
//       if (itemTime.isAfter(startOfToday)) todaysConsumption += energy;
//       if (itemTime.isAfter(startOfYesterday) && itemTime.isBefore(startOfToday))
//         yesterdaysConsumption += energy;
//       if (itemTime.isAfter(startOfThisMonth)) thisMonthsConsumption += energy;
//       if (deviceName) runningDevices.add(deviceName);

//       // Daily energy usage for the last 31 days
//       if (itemTime.isBetween(startOf31DaysAgo, endOfToday, null, "[]")) {
//         const dayIndex = itemTime.diff(startOf31DaysAgo, "days");
//         dailyUsage[dayIndex] += energy;

//         // Daily usage per room/device for the last 31 days
//         if (!dailyEnergyUsage[deviceName]) {
//           dailyEnergyUsage[deviceName] = new Array(31).fill(0);
//         }
//         dailyEnergyUsage[deviceName][dayIndex] += energy;
//       }

//       // Monthly energy usage
//       const monthIndex = itemTime.month(); // 0-based index for month
//       monthlyUsage[monthIndex] += energy;
//     });

//     // Prepare the summary object with all the data
//     const summary = {
//       consumptionSummary: {
//         todaysConsumption: todaysConsumption.toFixed(2) + " kWh",
//         yesterdaysConsumption: yesterdaysConsumption.toFixed(2) + " kWh",
//         thisMonthsConsumption: thisMonthsConsumption.toFixed(2) + " kWh",
//         runningDevicesCount: runningDevices.size,
//       },
//       dailyEnergyUsage: dailyUsage,
//       monthlyEnergyUsage: monthlyUsage,
//       dailyEnergyUsagePerRoom: dailyEnergyUsage,
//     };

//     res.json(summary);
//   } catch (error) {
//     console.error("Error retrieving energy usage summary:", error);
//     res.status(500).json({ error: "Failed to retrieve energy usage summary." });
//   }
// });

// app.get("/api/energy-usage-summary", limiter, async (req, res) => {
//   try {
//     console.log("Requesting energy usage summary...");

//     // Fetch the hourly, daily, and monthly data using your new functions
//     const monthlyData = await querySensorDataHourlyMonthly(); // Monthly energy usage data
//     const dailyData = await querySensorDataHourlyDaily(); // Daily energy usage data
//     const todayData = await queryMaxEnergyToday(); // Today's max energy data

//     const today = dayjs();
//     const startOfToday = today.startOf("day");
//     const startOfYesterday = today.subtract(1, "day").startOf("day");
//     const startOfThisMonth = today.startOf("month");
//     const startOf31DaysAgo = today.subtract(30, "days").startOf("day");
//     const endOfToday = today.endOf("day");

//     // Initialize variables for consumption summaries
//     let todaysConsumption = 0;
//     let yesterdaysConsumption = 0;
//     let thisMonthsConsumption = 0;
//     const runningDevices = new Set();
//     const dailyUsage = new Array(31).fill(0); // For daily usage over the last 31 days
//     const monthlyUsage = new Array(12).fill(0); // For monthly usage
//     const dailyEnergyUsage = {}; // For daily usage per device

//     // Process monthly data for energy usage
//     monthlyData.forEach((item) => {
//       const itemMonth = dayjs(item.month);
//       const deviceName = item.device_name;
//       const energy = parseFloat(item.highest_energy); // Assuming the energy is the "highest_energy" for the month

//       if (itemMonth.isAfter(startOfThisMonth)) {
//         thisMonthsConsumption += energy;
//       }

//       if (deviceName) {
//         runningDevices.add(deviceName);
//       }

//       const monthIndex = itemMonth.month(); // 0-based index for month
//       monthlyUsage[monthIndex] += energy;
//     });

//     // Process daily data for energy usage
//     dailyData.forEach((item) => {
//       const itemDay = dayjs(item.day);
//       const deviceName = item.device_name;
//       const energy = parseFloat(item.highest_energy); // Assuming the energy is the "highest_energy" for the day

//       if (itemDay.isAfter(startOfToday)) {
//         todaysConsumption += energy;
//       }

//       if (itemDay.isAfter(startOfYesterday) && itemDay.isBefore(startOfToday)) {
//         yesterdaysConsumption += energy;
//       }

//       if (deviceName) {
//         runningDevices.add(deviceName);
//       }

//       // Daily energy usage for the last 31 days
//       if (itemDay.isBetween(startOf31DaysAgo, endOfToday, null, "[]")) {
//         const dayIndex = itemDay.diff(startOf31DaysAgo, "days");
//         dailyUsage[dayIndex] += energy;

//         if (!dailyEnergyUsage[deviceName]) {
//           dailyEnergyUsage[deviceName] = new Array(31).fill(0);
//         }
//         dailyEnergyUsage[deviceName][dayIndex] += energy;
//       }
//     });

//     // Process today's energy data (max energy)
//     todayData.forEach((item) => {
//       const deviceName = item.device_name;
//       const energy = parseFloat(item.max_energy); // Assuming the energy is the "max_energy" for today

//       todaysConsumption += energy;

//       if (deviceName) {
//         runningDevices.add(deviceName);
//       }
//     });

//     // Prepare the summary object with all the data
//     const summary = {
//       consumptionSummary: {
//         todaysConsumption: todaysConsumption.toFixed(2) + " kWh",
//         yesterdaysConsumption: yesterdaysConsumption.toFixed(2) + " kWh",
//         thisMonthsConsumption: thisMonthsConsumption.toFixed(2) + " kWh",
//         runningDevicesCount: runningDevices.size,
//       },
//       dailyEnergyUsage: dailyUsage,
//       monthlyEnergyUsage: monthlyUsage,
//       dailyEnergyUsagePerRoom: dailyEnergyUsage,
//     };

//     res.json(summary);
//   } catch (error) {
//     console.error("Error retrieving energy usage summary:", error);
//     res.status(500).json({ error: "Failed to retrieve energy usage summary." });
//   }
// });

app.get("/api/energy-usage-summary", limiter, async (req, res) => {
  try {
    console.log("Requesting energy usage summary...");

    // Set today in Philippine Time
    const today = dayjs().tz("Asia/Manila");
    const startOfToday = today.startOf("day");
    const startOfYesterday = today.subtract(1, "day").startOf("day");
    const startOfThisMonth = today.startOf("month");
    const startOf31DaysAgo = today.subtract(30, "days").startOf("day");
    const endOfToday = today.endOf("day");

    // Rest of your code...
    // Fetch the hourly, daily, and monthly data using your new functions
    const monthlyData = await querySensorDataHourlyMonthly(); // Monthly energy usage data
    const dailyData = await querySensorDataHourlyDaily(); // Daily energy usage data
    const todayData = await queryMaxEnergyToday(); // Today's max energy data

    // Initialize variables for consumption summaries
    let todaysConsumption = 0;
    let yesterdaysConsumption = 0;
    let thisMonthsConsumption = 0;
    const runningDevices = new Set();
    const dailyUsage = new Array(31).fill(0); // For daily usage over the last 31 days
    const monthlyUsage = new Array(12).fill(0); // For monthly usage
    const dailyEnergyUsage = {}; // For daily usage per device

    // Process monthly data for energy usage
    monthlyData.forEach((item) => {
      const itemMonth = dayjs(item.month).tz("Asia/Manila");
      const deviceName = item.device_name;
      const energy = parseFloat(item.highest_energy); // Assuming the energy is the "highest_energy" for the month

      if (itemMonth.isAfter(startOfThisMonth)) {
        thisMonthsConsumption += energy;
      }

      if (deviceName) {
        runningDevices.add(deviceName);
      }

      const monthIndex = itemMonth.month(); // 0-based index for month
      monthlyUsage[monthIndex] += energy;
    });

    // Process daily data for energy usage
    dailyData.forEach((item) => {
      const itemDay = dayjs(item.day).tz("Asia/Manila");
      const deviceName = item.device_name;
      const energy = parseFloat(item.highest_energy); // Assuming the energy is the "highest_energy" for the day

      if (itemDay.isAfter(startOfToday)) {
        todaysConsumption += energy;
      }

      if (itemDay.isAfter(startOfYesterday) && itemDay.isBefore(startOfToday)) {
        yesterdaysConsumption += energy;
      }

      if (deviceName) {
        runningDevices.add(deviceName);
      }

      // Daily energy usage for the last 31 days
      if (itemDay.isBetween(startOf31DaysAgo, endOfToday, null, "[]")) {
        const dayIndex = itemDay.diff(startOf31DaysAgo, "days");
        dailyUsage[dayIndex] += energy;

        if (!dailyEnergyUsage[deviceName]) {
          dailyEnergyUsage[deviceName] = new Array(31).fill(0);
        }
        dailyEnergyUsage[deviceName][dayIndex] += energy;
      }
    });

    // // Process today's energy data (max energy)
    // todayData.forEach((item) => {
    //   const deviceName = item.device_name;
    //   const energy = parseFloat(item.max_energy); // Assuming the energy is the "max_energy" for today
    //   console.log(energy);
    //   todaysConsumption += energy;
    //   console.log(todaysConsumption);
    //   if (deviceName) {
    //     runningDevices.add(deviceName);
    //   }
    // });

    const formatEnergy = (value) =>
      value >= 1000
        ? (value / 1000).toFixed(2) + " kWh"
        : value.toFixed(2) + " Wh";

    const summary = {
      consumptionSummary: {
        todaysConsumption: formatEnergy(todaysConsumption),
        yesterdaysConsumption: formatEnergy(yesterdaysConsumption),
        thisMonthsConsumption: formatEnergy(thisMonthsConsumption),
        runningDevicesCount: runningDevices.size,
      },
      dailyEnergyUsage: dailyUsage,
      monthlyEnergyUsage: monthlyUsage,
      dailyEnergyUsagePerRoom: dailyEnergyUsage,
    };

    res.json(summary);
  } catch (error) {
    console.error("Error retrieving energy usage summary:", error);
    res.status(500).json({ error: "Failed to retrieve energy usage summary." });
  }
});

app.delete("/api/sensors", async (req, res) => {
  const rowsToDelete = req.body; // Expect an array of objects with id and deviceName
  console.log(rowsToDelete);
  if (!Array.isArray(rowsToDelete) || rowsToDelete.length === 0) {
    return res.status(400).json({ error: "Invalid or empty delete request." });
  }

  try {
    for (const row of rowsToDelete) {
      // Ensure both 'id' and 'deviceName' are present
      if (!row["id"] || !row["device_name"]) {
        throw new Error("Missing id or deviceName for deletion.");
      }

      const idsToDelete = Array.isArray(row["id"]) ? row["id"] : [row["id"]];

      // Loop through each ID and delete it
      for (const id of idsToDelete) {
        await deleteSensorData(id, row["device_name"]);
      }
    }
    res.status(200).json({ message: "Selected data deleted successfully." });
  } catch (error) {
    console.error(`Error deleting data: ${error.message}`);
    res.status(500).json({ error: error.message });
  }
});

// Schedule the task to run at midnight every day
cron.schedule("0 0 * * *", () => {
  calculateAndSaveAllAverages();
});

// Start server
app.listen(port, () => {
  console.log(`Server is running at http://localhost:${port}`);
  calculateAndSaveAllAverages();
});

// TODO: Implement hourly in default in loading data to table.
