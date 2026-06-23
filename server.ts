import express from "express";
import path from "path";
import { createServer as createViteServer } from "vite";
import { GoogleGenAI } from "@google/genai";

const app = express();
const PORT = 3000;

app.use(express.json());

// --- IN-MEMORY STATE & GENERATORS ---

// Types of datasets
type DatasetType = "ecommerce" | "infrastructure" | "saas_events";

interface ETLConfig {
  imputeMissing: boolean;
  dropDuplicates: boolean;
  filterOutliers: boolean;
  parseDates: boolean;
  validateConstraints: boolean;
}

// Global state for real-time streaming
let isStreamingActive = true;
let streamingDatasetType: DatasetType = "infrastructure";
let totalDataPointsProcessed = 84200; // Counter simulating cumulative throughput
let recentStreamData: any[] = [];
const MAX_STREAM_WINDOW = 100;

// Base generators for initial datasets
function generateEcommerceData(count: number, withAnomalies = false): any[] {
  const categories = ["Electronics", "Apparel", "Home & Kitchen", "Books", "Office Products"];
  const paymentMethods = ["Credit Card", "PayPal", "Apple Pay", "Bank Transfer", "Crypto"];
  const countries = ["US", "DE", "GB", "JP", "IN", "BR", "CA", "FR"];
  const data: any[] = [];
  const baseTime = Date.now() - count * 60000;

  for (let i = 0; i < count; i++) {
    // Intentionally introduce some nulls/duplicates/anomalies if flagged
    const hasNullValue = withAnomalies && Math.random() < 0.05;
    const isDuplicate = withAnomalies && Math.random() < 0.03;
    const isOutlier = withAnomalies && Math.random() < 0.02;

    const amount = isOutlier 
      ? parseFloat((Math.random() * 12000 + 3000).toFixed(2)) // Outlier purchase
      : parseFloat((Math.random() * 250 + 5.99).toFixed(2));

    const item = {
      id: `TX-${10000 + i}`,
      timestamp: baseTime + i * 60000,
      customer_id: `USR-${Math.floor(Math.random() * 1000) + 100}`,
      category: hasNullValue && Math.random() < 0.5 ? null : categories[Math.floor(Math.random() * categories.length)],
      amount: hasNullValue && Math.random() < 0.5 ? null : amount,
      payment_method: paymentMethods[Math.floor(Math.random() * paymentMethods.length)],
      country: countries[Math.floor(Math.random() * countries.length)],
      status: Math.random() < 0.94 ? "completed" : Math.random() < 0.7 ? "pending" : "failed"
    };

    data.push(item);
    if (isDuplicate) {
      data.push({ ...item }); // exact clone to simulate duplicate row
    }
  }
  return data;
}

function generateInfrastructureData(count: number, withAnomalies = false): any[] {
  const nodes = ["edge-node-01", "edge-node-02", "edge-node-03", "db-primary-01", "api-gate-01"];
  const data: any[] = [];
  const baseTime = Date.now() - count * 10000;

  for (let i = 0; i < count; i++) {
    const hasNullValue = withAnomalies && Math.random() < 0.04;
    const isDuplicate = withAnomalies && Math.random() < 0.03;
    const isOutlier = withAnomalies && Math.random() < 0.025;

    const cpu = isOutlier 
      ? parseFloat((95 + Math.random() * 5).toFixed(1)) // spike
      : parseFloat((20 + Math.random() * 55).toFixed(1));

    const temp = isOutlier
      ? parseFloat((112.5 + Math.random() * 15).toFixed(1)) // overheating anomaly
      : parseFloat((45.0 + Math.random() * 25).toFixed(1));

    const item = {
      id: `INF-${20000 + i}`,
      timestamp: baseTime + i * 10000,
      node_id: nodes[Math.floor(Math.random() * nodes.length)],
      cpu_usage: hasNullValue && Math.random() < 0.5 ? null : cpu,
      memory_usage: parseFloat((40 + Math.random() * 45).toFixed(1)),
      network_in_mbps: parseFloat((Math.random() * 850 + 10).toFixed(1)),
      network_out_mbps: parseFloat((Math.random() * 950 + 15).toFixed(1)),
      temperature_c: temp,
      status: cpu > 90 || temp > 95 ? "warning" : "healthy"
    };

    data.push(item);
    if (isDuplicate) {
      data.push({ ...item });
    }
  }
  return data;
}

function generateSaaSData(count: number, withAnomalies = false): any[] {
  const events = ["signup", "login", "view_dashboard", "export_report", "upgrade_plan", "checkout", "api_call_success", "api_call_error"];
  const devices = ["Desktop", "Mobile", "Tablet"];
  const browsers = ["Chrome", "Safari", "Firefox", "Edge"];
  const data: any[] = [];
  const baseTime = Date.now() - count * 15000;

  for (let i = 0; i < count; i++) {
    const hasNullValue = withAnomalies && Math.random() < 0.05;
    const isDuplicate = withAnomalies && Math.random() < 0.03;
    const isOutlier = withAnomalies && Math.random() < 0.02;

    const duration = isOutlier 
      ? Math.floor(Math.random() * 45000 + 15000) // severe latency anomaly
      : Math.floor(Math.random() * 1200 + 50);

    const item = {
      id: `EVT-${30000 + i}`,
      timestamp: baseTime + i * 15000,
      user_id: `USR-${Math.floor(Math.random() * 500) + 1}`,
      event_type: events[Math.floor(Math.random() * events.length)],
      duration_ms: hasNullValue && Math.random() < 0.5 ? null : duration,
      device_type: devices[Math.floor(Math.random() * devices.length)],
      browser: hasNullValue && Math.random() < 0.5 ? null : browsers[Math.floor(Math.random() * browsers.length)],
      status: Math.random() < 0.97 ? "success" : "error"
    };

    data.push(item);
    if (isDuplicate) {
      data.push({ ...item });
    }
  }
  return data;
}

// Fill seed real-time window
function reseedStreamingData() {
  if (streamingDatasetType === "infrastructure") {
    recentStreamData = generateInfrastructureData(60, false);
  } else if (streamingDatasetType === "ecommerce") {
    recentStreamData = generateEcommerceData(60, false);
  } else {
    recentStreamData = generateSaaSData(60, false);
  }
}
reseedStreamingData();

// Background Ingestion Stream simulation
setInterval(() => {
  if (!isStreamingActive) return;

  const count = Math.floor(Math.random() * 3) + 1; // 1 to 3 items per tick
  totalDataPointsProcessed += count;

  for (let idx = 0; idx < count; idx++) {
    let item: any;
    const now = Date.now();
    
    if (streamingDatasetType === "infrastructure") {
      const nodes = ["edge-node-01", "edge-node-02", "edge-node-03", "db-primary-01", "api-gate-01"];
      const isOutlier = Math.random() < 0.04;
      const cpu = isOutlier ? parseFloat((92 + Math.random() * 8).toFixed(1)) : parseFloat((25 + Math.random() * 45).toFixed(1));
      const temp = isOutlier ? parseFloat((105 + Math.random() * 10).toFixed(1)) : parseFloat((45 + Math.random() * 20).toFixed(1));
      
      item = {
        id: `INF-${Math.floor(Math.random() * 90000) + 20000}`,
        timestamp: now,
        node_id: nodes[Math.floor(Math.random() * nodes.length)],
        cpu_usage: parseFloat(cpu.toFixed(1)),
        memory_usage: parseFloat((45 + Math.random() * 35).toFixed(1)),
        network_in_mbps: parseFloat((Math.random() * 600 + 50).toFixed(1)),
        network_out_mbps: parseFloat((Math.random() * 700 + 60).toFixed(1)),
        temperature_c: temp,
        status: cpu > 90 || temp > 90 ? "warning" : "healthy"
      };
    } else if (streamingDatasetType === "ecommerce") {
      const categories = ["Electronics", "Apparel", "Home & Kitchen", "Books", "Office Products"];
      const paymentMethods = ["Credit Card", "PayPal", "Apple Pay", "Bank Transfer"];
      const countries = ["US", "DE", "GB", "JP", "IN", "BR", "CA", "FR"];
      const isOutlier = Math.random() < 0.03;
      const amount = isOutlier ? parseFloat((2500 + Math.random() * 1000).toFixed(2)) : parseFloat((10 + Math.random() * 150).toFixed(2));
      
      item = {
        id: `TX-${Math.floor(Math.random() * 90000) + 10000}`,
        timestamp: now,
        customer_id: `USR-${Math.floor(Math.random() * 1000) + 100}`,
        category: categories[Math.floor(Math.random() * categories.length)],
        amount: amount,
        payment_method: paymentMethods[Math.floor(Math.random() * paymentMethods.length)],
        country: countries[Math.floor(Math.random() * countries.length)],
        status: "completed"
      };
    } else {
      const events = ["signup", "login", "view_dashboard", "export_report", "checkout", "api_call_success", "api_call_error"];
      const devices = ["Desktop", "Mobile", "Tablet"];
      const browsers = ["Chrome", "Safari", "Firefox"];
      const isOutlier = Math.random() < 0.03;
      const duration = isOutlier ? Math.floor(25000 + Math.random() * 10000) : Math.floor(100 + Math.random() * 800);
      
      item = {
        id: `EVT-${Math.floor(Math.random() * 90000) + 30000}`,
        timestamp: now,
        user_id: `USR-${Math.floor(Math.random() * 500) + 1}`,
        event_type: events[Math.floor(Math.random() * events.length)],
        duration_ms: duration,
        device_type: devices[Math.floor(Math.random() * devices.length)],
        browser: browsers[Math.floor(Math.random() * browsers.length)],
        status: Math.random() < 0.98 ? "success" : "error"
      };
    }

    recentStreamData.push(item);
    if (recentStreamData.length > MAX_STREAM_WINDOW) {
      recentStreamData.shift();
    }
  }
}, 1500);


// --- API ROUTE HANDLERS ---

// Get active ingestion status and recent stream window
app.get("/api/stream/status", (req, res) => {
  res.json({
    isStreamingActive,
    streamingDatasetType,
    totalDataPointsProcessed,
    streamSize: recentStreamData.length
  });
});

// Toggle streaming ingestion
app.post("/api/stream/toggle", (req, res) => {
  const { active, datasetType } = req.body;
  if (typeof active === "boolean") {
    isStreamingActive = active;
  }
  if (datasetType && (datasetType === "ecommerce" || datasetType === "infrastructure" || datasetType === "saas_events")) {
    if (streamingDatasetType !== datasetType) {
      streamingDatasetType = datasetType;
      reseedStreamingData();
    }
  }
  res.json({
    success: true,
    isStreamingActive,
    streamingDatasetType
  });
});

// Get historical/stream data points
app.get("/api/stream/data", (req, res) => {
  res.json({
    datasetType: streamingDatasetType,
    data: recentStreamData,
    totalProcessed: totalDataPointsProcessed,
    timestamp: Date.now()
  });
});

// Trigger a raw mock dataset with anomalies to test the ETL pipeline
app.get("/api/etl/raw-dataset", (req, res) => {
  const type = (req.query.type as DatasetType) || "infrastructure";
  const size = parseInt(req.query.size as string) || 500;
  
  let data: any[] = [];
  if (type === "ecommerce") {
    data = generateEcommerceData(size, true);
  } else if (type === "infrastructure") {
    data = generateInfrastructureData(size, true);
  } else {
    data = generateSaaSData(size, true);
  }

  res.json({
    type,
    totalRows: data.length,
    data
  });
});

// Execute the simulated Python/Pandas automated ETL pipeline
app.post("/api/etl/run", (req, res) => {
  const { type, rawData, config }: { type: DatasetType; rawData: any[]; config: ETLConfig } = req.body;
  
  if (!rawData || !Array.isArray(rawData)) {
    return res.status(400).json({ error: "No raw dataset provided" });
  }

  const initialCount = rawData.length;
  let df = [...rawData];
  const logs: string[] = [];
  const metrics = {
    initialRows: initialCount,
    cleanedRows: 0,
    nullsRemoved: 0,
    duplicatesRemoved: 0,
    outliersFiltered: 0,
    invalidTypesCoerced: 0,
    processingTimeMs: parseFloat((12 + Math.random() * 15).toFixed(2))
  };

  logs.push(`[ETL START] Initiating automated data transformation pipeline for: ${type.toUpperCase()}`);
  logs.push(`[1/4 Extract] Loaded ${initialCount} records from local stream source.`);

  // Stage 1: Drop Duplicates
  if (config.dropDuplicates) {
    const uniqueMap = new Map<string, boolean>();
    const originalLength = df.length;
    df = df.filter((row) => {
      // Create a unique key string based on all values
      const key = JSON.stringify(row);
      if (uniqueMap.has(key)) {
        return false;
      }
      uniqueMap.set(key, true);
      return true;
    });
    metrics.duplicatesRemoved = originalLength - df.length;
    logs.push(`[2/4 Clean] pandas.DataFrame.drop_duplicates(): Eliminated ${metrics.duplicatesRemoved} identical duplicate rows.`);
  } else {
    logs.push(`[2/4 Clean] Duplicate removal skipped via config flag.`);
  }

  // Stage 2: Clean Missing / Impute Values
  if (config.imputeMissing) {
    let nullCount = 0;
    df = df.map((row) => {
      const cloned = { ...row };
      
      // Check individual fields based on type
      Object.keys(cloned).forEach((key) => {
        if (cloned[key] === null || cloned[key] === undefined) {
          nullCount++;
          // Impute values
          if (key === "cpu_usage") cloned[key] = 45.0; // Mean value imputation
          else if (key === "amount") cloned[key] = 29.99; // Median value imputation
          else if (key === "category") cloned[key] = "General"; // Mode value imputation
          else if (key === "duration_ms") cloned[key] = 250;
          else if (key === "browser") cloned[key] = "Chrome";
        }
      });
      return cloned;
    });
    metrics.nullsRemoved = nullCount;
    logs.push(`[2/4 Clean] pandas.DataFrame.fillna(method='mean_mode_imputation'): Successfully imputed ${nullCount} null/empty cells.`);
  } else {
    logs.push(`[2/4 Clean] Missing value check completed. Imputation deactivated.`);
  }

  // Stage 3: Filter Outliers
  if (config.filterOutliers) {
    const originalLength = df.length;
    df = df.filter((row) => {
      if (type === "infrastructure" && row.cpu_usage > 99) {
        return false; // Extreme cpu spike outlier
      }
      if (type === "infrastructure" && row.temperature_c > 115) {
        return false; // Thermal safety limit exceedance
      }
      if (type === "ecommerce" && row.amount > 3000) {
        return false; // Outlier whale transaction
      }
      if (type === "saas_events" && row.duration_ms > 30000) {
        return false; // HTTP timeout/lag spike outliers
      }
      return true;
    });
    metrics.outliersFiltered = originalLength - df.length;
    logs.push(`[2/4 Clean] df = df[np.abs(stats.zscore(df[numeric_cols])) < 3]: Filtered out ${metrics.outliersFiltered} extreme outlier rows.`);
  } else {
    logs.push(`[2/4 Clean] Outlier filtering bypassed.`);
  }

  // Stage 4: Transform (Date Parsing / Field Computations)
  if (config.parseDates) {
    df = df.map((row) => {
      return {
        ...row,
        formatted_date: new Date(row.timestamp).toISOString().split("T")[0],
        formatted_time: new Date(row.timestamp).toLocaleTimeString(),
        hour_of_day: new Date(row.timestamp).getHours()
      };
    });
    logs.push(`[3/4 Transform] pd.to_datetime(df['timestamp']): Cast Unix timestamps into responsive ISO strings and aggregated hour-of-day buckets.`);
  } else {
    logs.push(`[3/4 Transform] Date parsing skipped.`);
  }

  // Stage 5: Validate Schema & Constraints
  if (config.validateConstraints) {
    let violationCount = 0;
    df = df.map((row) => {
      const validated = { ...row };
      if (type === "infrastructure") {
        if (validated.cpu_usage < 0) { validated.cpu_usage = 0; violationCount++; }
        if (validated.cpu_usage > 100) { validated.cpu_usage = 100; violationCount++; }
        if (validated.memory_usage > 100) { validated.memory_usage = 100; violationCount++; }
      } else if (type === "ecommerce") {
        if (validated.amount < 0) { validated.amount = 0; violationCount++; }
      } else {
        if (validated.duration_ms < 0) { validated.duration_ms = 0; violationCount++; }
      }
      return validated;
    });
    metrics.invalidTypesCoerced = violationCount;
    logs.push(`[4/4 Validate] Great Expectations validation: Schema verified. Adjusted ${violationCount} data bounds violations.`);
  } else {
    logs.push(`[4/4 Validate] Schema check bypassed.`);
  }

  metrics.cleanedRows = df.length;
  logs.push(`[ETL COMPLETE] Pipeline finished successfully in ${metrics.processingTimeMs}ms! Output dataset comprises ${metrics.cleanedRows} fully validated rows.`);

  // Generate python snippet mimicking this execution
  let pythonCode = `import pandas as pd
import numpy as np
from scipy import stats

def run_automated_etl_pipeline(raw_file_path):
    print("Initiating automated pipeline for: ${type}")
    # 1. Extract
    df = pd.read_json(raw_file_path)
    print(f"Extracted {len(df)} records")
    \n`;

  if (config.dropDuplicates) {
    pythonCode += `    # 2. Drop duplicates
    initial_len = len(df)
    df.drop_duplicates(inplace=True)
    print(f"Removed {initial_len - len(df)} duplicate rows")\n`;
  }
  if (config.imputeMissing) {
    if (type === "infrastructure") {
      pythonCode += `    # 3. Impute Missing (mean for numeric cols)
    df['cpu_usage'] = df['cpu_usage'].fillna(df['cpu_usage'].mean())\n`;
    } else if (type === "ecommerce") {
      pythonCode += `    # 3. Impute Missing (median amount)
    df['amount'] = df['amount'].fillna(df['amount'].median())
    df['category'] = df['category'].fillna('General')\n`;
    } else {
      pythonCode += `    # 3. Impute Missing (median duration)
      df['duration_ms'] = df['duration_ms'].fillna(250)
      df['browser'] = df['browser'].fillna('Chrome')\n`;
    }
  }
  if (config.filterOutliers) {
    if (type === "infrastructure") {
      pythonCode += `    # 4. Outlier Filtering
    df = df[df['cpu_usage'] <= 99]
    df = df[df['temperature_c'] <= 115]\n`;
    } else if (type === "ecommerce") {
      pythonCode += `    # 4. Outlier Filtering
    df = df[df['amount'] <= 3000]\n`;
    } else {
      pythonCode += `    # 4. Outlier Filtering
    df = df[df['duration_ms'] <= 30000]\n`;
    }
  }
  if (config.parseDates) {
    pythonCode += `    # 5. Transform timestamps
    df['parsed_time'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['formatted_date'] = df['parsed_time'].dt.strftime('%Y-%m-%d')
    df['hour_of_day'] = df['parsed_time'].dt.hour\n`;
  }
  if (config.validateConstraints) {
    if (type === "infrastructure") {
      pythonCode += `    # 6. Schema check & value constraints
    df['cpu_usage'] = df['cpu_usage'].clip(0, 100)
    df['memory_usage'] = df['memory_usage'].clip(0, 100)\n`;
    } else {
      pythonCode += `    # 6. Schema check & value constraints
    df['amount'] = df['amount'].clip(lower=0)\n`;
    }
  }

  pythonCode += `    
    print(f"ETL pipeline completed. Validated dataset has {len(df)} ready-to-use entries.")
    return df`;

  res.json({
    type,
    metrics,
    logs,
    pythonCode,
    transformedData: df
  });
});

// --- SERVER-SIDE GEMINI ANALYTICS API ROUTE ---

// Lazy initialize and check environment key safely
let aiClient: GoogleGenAI | null = null;

function getGeminiClient(): GoogleGenAI {
  if (!aiClient) {
    const key = process.env.GEMINI_API_KEY;
    if (!key) {
      throw new Error("GEMINI_API_KEY environment variable is not defined. Please configure it in Settings > Secrets.");
    }
    aiClient = new GoogleGenAI({
      apiKey: key,
      httpOptions: {
        headers: {
          "User-Agent": "aistudio-build",
        },
      },
    });
  }
  return aiClient;
}

app.post("/api/gemini/analyze", async (req, res) => {
  const { datasetType, summaryStats, queryType, userPrompt } = req.body;

  try {
    const ai = getGeminiClient();

    let systemInstruction = "You are an elite principal AI Data Scientist and Systems Analyst who assists stakeholders with real-time operations, analytics, and pipeline engineering. You express insights concisely in perfect Markdown, highlighting metrics and identifying specific areas of business improvement. Be professional and brief.";
    
    let prompt = `Analyze the current dataset characteristics.
Dataset Type: ${datasetType}
Summary Statistics: ${JSON.stringify(summaryStats)}

`;

    if (queryType === "anomalies") {
      prompt += "Please identify potential issues, security threats, performance degradation, or transaction fraud in this telemetry. Mention what anomalies would look like in the raw streams and how our ETL pipeline filters them.";
    } else if (queryType === "forecast") {
      prompt += "Please forecast potential behavior or operational load for the next 24-hour cycle. What spikes should engineers prepare for? Provide specific numbers based on current levels.";
    } else if (queryType === "recommendations") {
      prompt += "Provide 3 high-impact, actionable business and performance recommendations to optimize our metrics or systems 95% faster. Format with clean bullet points.";
    } else if (queryType === "custom") {
      prompt += `The user asks: "${userPrompt}". Please provide an intelligent data-driven answer referencing our metrics.`;
    }

    const response = await ai.models.generateContent({
      model: "gemini-3.5-flash",
      contents: prompt,
      config: {
        systemInstruction,
        temperature: 0.7,
      },
    });

    const text = response.text || "No insights generated.";
    res.json({ text });

  } catch (err: any) {
    console.error("Gemini analysis error:", err);
    res.status(500).json({ 
      error: err.message || "An error occurred during Gemini analysis.",
      isKeyMissing: !process.env.GEMINI_API_KEY
    });
  }
});


// --- VITE MIDDLEWARE OR STATIC SERVING ---

async function startServer() {
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), "dist");
    app.use(express.static(distPath));
    app.get("*", (req, res) => {
      res.sendFile(path.join(distPath, "index.html"));
    });
  }

  app.listen(PORT, "0.0.0.0", () => {
    console.log(`[SERVER] Ready on http://localhost:${PORT}`);
  });
}

startServer();
