#!/usr/bin/env node
var WebSocketClient = require('websocket').client;
var client = new WebSocketClient();

var 	kafka = require('no-kafka');
var	producer = new kafka.Producer();

producer.init().then(function(){
  return producer.send({
      topic: 'sensor',
      partition: 0,
      message: {
          value: '{"sensor":"light","values":{"value":-1.4268341},"type":"sensor"}'
      }
  });
})
.then(function (result) {
//console.log(result);
  /*
  [ { topic: 'kafka-test-topic', partition: 0, offset: 353 } ]
  */
});



client.on('connectFailed', function(error) {
    console.log('Connect Error: ' + error.toString());
});
 
client.on('connect', function(connection) {
    console.log('WebSocket Client Connected');
    connection.on('error', function(error) {
        console.log("Connection Error: " + error.toString());
    });
    connection.on('close', function() {
        console.log('echo-protocol Connection Closed');
    });
    connection.on('message', function(message) {
        if (message.type === 'utf8') {
            console.log("Received");
		producer.send({topic:'sensor',partition:0,message:{value:message.utf8Data}});
        }
    });
    
    function sendNumber() {
        if (connection.connected) {
            var number = Math.round(Math.random() * 0xFFFFFF);
            connection.sendUTF(number.toString());
            setTimeout(sendNumber, 1000);
        }
    }
    sendNumber();
});
 
client.connect('ws://192.168.43.1:8081/', 'echo-protocol');
