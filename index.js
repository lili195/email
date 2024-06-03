const { Client } = require('pg');
const express = require('express');
const axios = require('axios');
const PDFDocument = require('pdfkit');
const fs = require('fs')
require('dotenv').config()
const jpeg = require('jpeg-js')

const amqp = require('amqplib/callback_api');

const consumeMessages = () => {
    amqp.connect('amqp://192.168.0.102:5672', (err, connection) => {
        if (err) {
            console.error('Connection error:', err);
            throw err;
        }

        connection.createChannel((err, channel) => {
            if (err) {
                console.error('Channel creation error:', err);
                throw err;
            }

            const queue = 'tasks_queue';

            channel.assertQueue(queue, {
                durable: true,
            });

            // Prefetch to ensure only one message is handled at a time per consumer
            channel.prefetch(1);

            console.log('Waiting for messages in %s', queue);

            channel.consume(queue, async (msg) => {
                if (msg !== null) {
                    try {
                        const messageContent = JSON.parse(msg.content.toString());
                        const { keyword, email } = messageContent;
                        console.log(`Received: keyword=${keyword}, email=${email}`);

                        // Simulating message processing, replace with your actual logic
                        //await createReport(keyword);
                        console.log(`procesando mensaje ${keyword}`)
                        await createReport(keyword);

                        channel.ack(msg);
                        console.log(`Processed: keyword=${keyword}, email=${email}`);
                    } catch (error) {
                        console.error('Error processing message:', error);
                        // Optional: channel.nack(msg, false, true); // requeue the message in case of error
                    }
                }
            }, {
                noAck: false, // Ensure acknowledgements are required
            });
        });
    });
};

consumeMessages();


const client = new Client({
    host: 'localhost',
    port: 5432,
    database: 'postgres',
    user: 'postgres',
    password: '123',
});

client.connect((err) => {
    if (err) {
        console.error('Error de conexión', err.stack);
    } else {
        console.log('Conectado');
    }
});

const app = express();
const port = process.env.PORT;


const createReport = async (keyword) => {
    console.log(`Iniciando proceso de creacion de pdf, palabra recibida ${keyword}`)
    try {

        const buffers = await getAllImageBuffers(); // Obtener los buffers de las imágenes

        const arrowNum = buffers.length;

        console.log(arrowNum)

        const result = await client.query(`SELECT * FROM users WHERE email LIKE '%${keyword}%' LIMIT ${arrowNum}`);
        console.log('Datos obtenidos de la DB exitosamente!');

        const filePath = `./report-${keyword}.pdf`;
        await createPDF(filePath, result.rows, buffers);
        console.log(`PDF guardado en ${filePath}`);
    } catch (error) {
        console.error('Error ejecutando la consulta', error.stack);
    }
}


const getImagesUrl = async () => {
    let isNotJPG = true;
    while (isNotJPG) {
        try {
            const response = await axios.get("https://api.thecatapi.com/v1/images/search?limit=100", {
                headers: {
                    'x-api-key': 'live_Ree0qajxpY8ntiBmPLG0vgJtHByWFLfP7FB6UXozaAopOJScgKaIWQSEGfJiLezl'
                }
            });
            const imageUrls = response.data.map(item => item.url);
            const jpgUrls = imageUrls.filter(url => url.endsWith('.jpg'));
            return jpgUrls;
        } catch (error) {
            console.error(`Error obteniendo la imagen`, error.stack);
        }
    }
}


const getImgBytes = async (imageUrl) => {
    const maxRetries = 3;
    let attempts = 0;
    while (attempts < maxRetries) {
        try {
            const response = await axios.get(imageUrl, { responseType: 'arraybuffer' });
            const buffer = Buffer.from(response.data);

            // Verificar si el buffer es una imagen JPEG válida
            const isValidJPEG = jpeg.decode(buffer, true);
            if (!isValidJPEG) {
                throw new Error('Invalid JPEG');
            }

            console.log("Buffer de imagen recuperado");
            return buffer;
        } catch (error) {
            attempts++;
            console.error(`Error obteniendo los bytes de la imagen, intento ${attempts} de ${maxRetries}`, error.stack);
            if (attempts >= maxRetries) {
                throw error;
            }
        }
    }
};

// Función para obtener los buffers de todas las imágenes
const getAllImageBuffers = async () => {
    try {
        const { default: pLimit } = await import('p-limit'); // Importación dinámica
        const urls = await getImagesUrl();
        const limit = pLimit(5); // Límite de 5 solicitudes en paralelo
        const promises = urls.map(url => limit(() => getImgBytes(url)));
        const buffers = await Promise.all(promises);
        console.log("Las imágenes se han recuperado correctamente");
        return buffers;
    } catch (error) {
        console.error("Error obteniendo los buffers de las imágenes", error.stack);
    }
};


const createPDF = async (filePath, data, buffers) => {
    try {
        const doc = new PDFDocument();
        const stream = fs.createWriteStream(filePath);

        doc.pipe(stream);

        for (let i = 0; i < data.length; i++) {
            const rowData = data[i];
            const buffer = buffers[i];

            // Escribir datos de la fila
            doc.text(JSON.stringify(rowData));

            // Insertar imagen
            if (buffer) {
                doc.image(buffer, { width: 200 });
            } else {
                doc.text("No se pudo obtener la imagen.");
            }

            // Agregar un salto de página después de cada fila
            if (i !== data.length - 1) {
                doc.addPage();
            }
        }

        console.log('PDF creado')
        doc.end();

        // // Asegurarse de que el flujo se cierre correctamente
        // await new Promise((resolve, reject) => {
        //     stream.on('finish', resolve);
        //     stream.on('error', reject);
        // });
    } catch (error) {
        console.error('Error creando el pdf', error.stack);
    }
}


app.listen(port, () => {
    console.log(`App escuchando en el puerto ${port}`);
});