// Impor library yang dibutuhkan
const mqtt = require('mqtt');
const mysql = require('mysql2/promise');

console.log("🚀 [SERVER] Memulai skrip listener...");


const MQTT_CONFIG = {
    brokerUrl: process.env.MQTT_BROKER_URL || 'mqtt://broker.hivemq.com:1883',
    topic: process.env.MQTT_TOPIC || 'skripsi/ruang1/sensor'
};

const DB_CONFIG = {
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE
};


// Variabel 'memori' untuk menyimpan status terakhir
let lastReceivedData = {
    suhu: null,
    gas: null,
    asap: null,
    status_api: null,
    kipas: null,
    buzzer: null,
    pompa: null
};

// Cek apakah konfigurasi database sudah lengkap
if (!DB_CONFIG.host || !DB_CONFIG.user || !DB_CONFIG.database) {
    console.error("❌ [DB] Konfigurasi database tidak lengkap! Pastikan DB_HOST, DB_USER, dan DB_DATABASE sudah di-set di Environment Variables.");
    process.exit(1); // Hentikan skrip jika konfigurasi tidak ada
}


console.log(`[MQTT] Mencoba terhubung ke Broker di ${MQTT_CONFIG.brokerUrl}`);
const mqttClient = mqtt.connect(MQTT_CONFIG.brokerUrl);

mqttClient.on('connect', () => {
    console.log('✅ [MQTT] Berhasil terhubung ke Broker!');
    mqttClient.subscribe(MQTT_CONFIG.topic, (err) => {
        if (!err) {
            console.log(`👂 [MQTT] Berhasil subscribe dan siap mendengarkan topik: "${MQTT_CONFIG.topic}"`);
        } else {
            console.error('❌ [MQTT] Gagal melakukan subscribe:', err);
        }
    });
});

mqttClient.on('message', async (topic, message) => {
    
    const payloadString = message.toString();

    try {
        const newData = JSON.parse(payloadString);

        // --- Definisikan ambang batas LONJAKAN nilai (bukan nilai absolut) ---
        const SUHU_JUMP_THRESHOLD = 2.0;
        const GAS_JUMP_THRESHOLD = 10;
        const ASAP_JUMP_THRESHOLD = 5;

        let shouldSave = false;

        if (lastReceivedData.suhu === null) {
            shouldSave = true;
            console.log(`\n✅ Data pertama diterima. Menyimpan...`);
        } else {
            const isStateChange = 
                newData.kipas !== lastReceivedData.kipas ||
                newData.buzzer !== lastReceivedData.buzzer ||
                newData.pompa !== lastReceivedData.pompa ||
                newData.status_api !== lastReceivedData.status_api;

            const isSignificantJump = 
                Math.abs(newData.suhu - lastReceivedData.suhu) >= SUHU_JUMP_THRESHOLD ||
                Math.abs(newData.gas - lastReceivedData.gas) >= GAS_JUMP_THRESHOLD ||
                Math.abs(newData.asap - lastReceivedData.asap) >= ASAP_JUMP_THRESHOLD;

            if (isStateChange) {
                shouldSave = true;
                console.log(`\n✅ Perubahan status aktuator/api terdeteksi. Menyimpan...`);
            } else if (isSignificantJump) {
                shouldSave = true;
                console.log(`\n✅ Lonjakan nilai sensor terdeteksi. Menyimpan...`);
            }
        }

        if (shouldSave) {
            console.log(payloadString);

            // --- PERUBAHAN 2: KONEKSI DATABASE DIBUAT SEKALI DAN DIGUNAKAN KEMBALI ---
            // Ini lebih efisien daripada membuat koneksi baru setiap ada pesan.
            // Kita akan pindahkan pembuatan koneksi ke luar. (Lihat di bawah)

            const sqlQuery = `INSERT INTO sensor_data (suhu, gas, asap, status_api, kipas, buzzer, pompa, waktu)
                              VALUES (?, ?, ?, ?, ?, ?, ?, NOW())`; // Tambahkan NOW() untuk waktu
            const values = [
                newData.suhu, newData.gas, newData.asap, newData.status_api,
                newData.kipas, newData.buzzer, newData.pompa
            ];
            
            // Menggunakan koneksi yang sudah ada (pool)
            const connection = await mysql.createConnection(DB_CONFIG);
            const [result] = await connection.execute(sqlQuery, values);
            await connection.end(); // Tutup koneksi setelah selesai
            console.log(`👍 [DB] Data berhasil disimpan! ID baris baru: ${result.insertId}`);
        }
        
        lastReceivedData = newData;

    } catch (error) {
        console.error('❌ Terjadi error saat memproses pesan:', error);
    }
});


mqttClient.on('error', (error) => {
    console.error('❌ [MQTT] Terjadi error koneksi:', error);
});

mqttClient.on('close', () => {
    console.log('🔌 [MQTT] Koneksi ke Broker terputus.');
});