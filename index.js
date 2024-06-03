const { Client } = require('pg');
const express = require('express');
const axios = require('axios');
const PDFDocument = require('pdfkit');
const consumeMessages = require('./consumer')

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
    try {

        const buffers = await getAllImageBuffers(); // Obtener los buffers de las imágenes

        const arrowNum = buffers.length;

        console.log(arrowNum)

        const result = await client.query(`SELECT * FROM users WHERE email LIKE '%${keyword}%' LIMIT ${arrowNum}`);
        console.log('Datos obtenidos de la DB exitosamente!');

        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', 'attachment; filename=report.pdf');

        await createPDF(res, result.rows, buffers); 
    } catch (error) {
        console.error('Error ejecutando la consulta', error.stack);
        res.status(500).send(`Error ejecutando la consulta`); //para la palabra ${keyword}
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
    console.log("Imagen recuperada", imageUrl)
    const maxRetries = 3;
    let attempts = 0;
    while (attempts < maxRetries) {
        try {
            const response = await axios.get(imageUrl, { responseType: 'arraybuffer' });
            return response.data; // Retorna los datos de la imagen
        } catch (error) {
            attempts++;
            console.error(`Error obteniendo los bytes de la imagen, intento ${attempts} de ${maxRetries}`, error.stack);
            if (attempts >= maxRetries) {
                throw error;
            }
        }
    }
}

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

const createPDF = async (res, data, buffers) => {
    const doc = new PDFDocument();

    doc.pipe(res);

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
}


app.listen(port, () => {
    console.log(`App escuchando en el puerto ${port}`);
});