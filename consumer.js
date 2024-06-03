const amqp = require('amqplib/callback_api');

function consumeMessages() {
  amqp.connect('amqp://192.168.0.102:5672', (err, connection) => {
    if (err) throw err;
    connection.createChannel((err, channel) => {
      if (err) throw err;
      const queue = 'tasks_queue';
      channel.assertQueue(queue, {
        durable: true,
      });
      channel.prefetch(1);
      console.log('Waiting for messages in %s', queue);
      channel.consume(queue, (msg) => {
        const messageContent = JSON.parse(msg.content.toString());
        const { keyword, email } = messageContent;
        console.log(`Received: keyword=${keyword}, email=${email}`);
        
        channel.ack(msg);
        console.log(`Processed: keyword=${keyword}, email=${email}`);
      });
    });
  });
}

consumeMessages();

module.exports = consumeMessages;