// server.js - Node.js Backend (Hợp nhất MQTT Ingestion và API Server)

const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const mqtt = require('mqtt'); 

const app = express();
const PORT = 3000; // Cổng API Server

// Sử dụng middleware
app.use(express.json()); // Cho phép Express đọc body JSON
app.use(cors()); // Cho phép Frontend truy cập từ nguồn khác (CORS)

// --- 1. CẤU HÌNH DATABASE MONGODB ATLAS ---
// THAY THẾ CHUỖI KẾT NỐI NÀY VỚI USERNAME, PASSWORD, CLUSTER NAME CỦA BẠN
const MONGO_URI = 'mongodb+srv://<USERNAME_MONGODB>:<PASSWORD_MONGODB>@<CLUSTER_NAME>.mongodb.net/GasLeakDB?retryWrites=true&w=majority';

// --- 2. CẤU HÌNH MQTT HIVE MQ CLOUD ---
// THAY THẾ HOST VÀ THÔNG TIN ĐĂNG NHẬP CHÍNH XÁT CỦA BẠN (ĐÃ TẠO Ở ACCESS MANAGEMENT)
const MQTT_HOST = 'tcp://<HOST_NAME_TU_HIVEMQ_CLOUD>:1883'; 
const MQTT_OPTIONS = {
    clientId: 'NodeJS_Unified_Client',
    username: '<USERNAME_HOP_LE_TU_HIVEMQ_CLOUD>', // Ví dụ: nhom9_gas
    password: '<PASSWORD_HOP_LE_TU_HIVEMQ_CLOUD>' // Ví dụ: GasLeak@2025
};
const TOPIC_SUBSCRIBE_DATA = '/gas_leak/data'; // Topic nhận data từ ESP32
const TOPIC_PUBLISH_COMMAND = '/gas_leak/command'; // Topic gửi lệnh tới ESP32

let mqttClient; // Biến toàn cục cho kết nối MQTT

// --- 3. ĐỊNH NGHĨA SCHEMA MONGODB ---
const SensorDataSchema = new mongoose.Schema({
    gas_value: { type: Number, required: true },
    system_status: { type: Number, required: true }, // 0=An toan, 1=Canh bao, 2=Nguy hiem
    timestamp: { type: Date, default: Date.now, required: true }
});
const SensorData = mongoose.model('SensorData', SensorDataSchema); 

// ----------------------------------------------------------------------
// PHẦN A: XỬ LÝ KẾT NỐI VÀ DATA INGESTION (Nhận data từ MQTT và lưu DB)
// ----------------------------------------------------------------------
function setupMqttIngestion() {
    mqttClient = mqtt.connect(MQTT_HOST, MQTT_OPTIONS);

    mqttClient.on('connect', () => {
        console.log('MQTT Client connected successfully.');
        
        // Subscribe để lắng nghe dữ liệu từ ESP32
        mqttClient.subscribe(TOPIC_SUBSCRIBE_DATA, (err) => {
            if (!err) {
                console.log(`Subscribed to data topic: ${TOPIC_SUBSCRIBE_DATA}`);
            } else {
                console.error('Subscription error:', err);
            }
        });
    });

    mqttClient.on('error', (error) => {
        console.error('MQTT connection error:', error);
    });

    // Xử lý dữ liệu nhận được
    mqttClient.on('message', async (topic, message) => {
        if (topic === TOPIC_SUBSCRIBE_DATA) {
            try {
                // Payload ESP32 gửi: {"gas_value": 37, "status": 0}
                const data = JSON.parse(message.toString()); 
                
                const newSensorData = new SensorData({
                    gas_value: data.gas_value,
                    system_status: data.status,
                });
                
                await newSensorData.save();
                console.log(`[INGEST] Data saved to DB. Gas: ${data.gas_value}, Status: ${data.status}`);
            } catch (e) {
                console.error('[INGEST] Error parsing or saving data:', e.message);
            }
        }
    });
}

// ----------------------------------------------------------------------
// PHẦN B: RESTful API SERVER (Phục vụ Web Client)
// ----------------------------------------------------------------------

// API 1: Lấy trạng thái mới nhất
app.get('/api/status', async (req, res) => {
    try {
        // Tìm bản ghi mới nhất
        const latestData = await SensorData.findOne()
            .sort({ timestamp: -1 })
            .limit(1);

        if (latestData) {
            res.json({
                gas_value: latestData.gas_value,
                system_status: latestData.system_status,
                timestamp: latestData.timestamp
            });
        } else {
            res.status(404).json({ message: "No data found." });
        }
    } catch (error) {
        console.error("Error fetching status:", error);
        res.status(500).json({ message: "Internal Server Error" });
    }
});

// API 2: Lấy dữ liệu thống kê theo khoảng thời gian
app.get('/api/report', async (req, res) => {
    const { timeframe } = req.query; // Ví dụ: timeframe=7d (7 ngày)

    let startTime = new Date();
    
    // Tính toán thời gian bắt đầu
    if (timeframe === '7d') {
        startTime.setDate(startTime.getDate() - 7);
    } else if (timeframe === '30d' || timeframe === '1mo') {
        startTime.setMonth(startTime.getMonth() - 1);
    } else {
        // Mặc định là 24h
        startTime.setDate(startTime.getDate() - 1);
    }

    try {
        const reportData = await SensorData.find({
            timestamp: { $gte: startTime } 
        }).sort({ timestamp: 1 }); // Sắp xếp theo thời gian tăng dần

        res.json(reportData);
    } catch (error) {
        console.error("Error fetching report:", error);
        res.status(500).json({ message: "Internal Server Error" });
    }
});

// API 3: Nhận lệnh từ Web và gửi MQTT cho ESP32
app.post('/api/door/control', (req, res) => {
    const { action } = req.body; // body: { "action": "OPEN" } hoặc { "action": "CLOSE" }

    if (!mqttClient || !mqttClient.connected) {
        return res.status(503).json({ success: false, message: "MQTT Client not connected to Broker." });
    }

    let commandToSend;
    if (action === 'OPEN') {
        commandToSend = 'OPEN';
    } else if (action === 'CLOSE') {
        commandToSend = 'CLOSE';
    } else {
        return res.status(400).json({ success: false, message: "Invalid action. Must be OPEN or CLOSE." });
    }

    // Gửi lệnh lên Topic MQTT (ESP32 sẽ nhận)
    mqttClient.publish(TOPIC_PUBLISH_COMMAND, commandToSend, (err) => {
        if (err) {
            console.error('Error publishing command:', err);
            res.status(500).json({ success: false, message: "Failed to publish MQTT command." });
        } else {
            console.log(`[COMMAND] Command published to MQTT: ${commandToSend}`);
            res.json({ success: true, message: `Door command ${action} sent.` });
        }
    });
});

// ----------------------------------------------------------------------
// Khởi động Server
// ----------------------------------------------------------------------
async function startServer() {
    // 1. Kết nối MongoDB
    try {
        await mongoose.connect(MONGO_URI);
        console.log('MongoDB connected successfully.');
    } catch (error) {
        console.error('MongoDB connection error. Please check MONGO_URI.');
        return; 
    }
    
    // 2. Thiết lập MQTT Client (Ingestion & Command)
    setupMqttIngestion();

    // 3. Khởi động API Server
    app.listen(PORT, () => {
        console.log(`API Server listening on port ${PORT}`);
        console.log(`Test API Status: http://localhost:${PORT}/api/status`);
    });
}

startServer();