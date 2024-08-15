const express = require('express');
const WebSocket = require('ws');
const protobuf = require('protobufjs');
const UpstoxClient = require('upstox-js-sdk');
require('dotenv').config();

const app = express();
const HTTP_PORT = 3000; // Port for the HTTP server
const WS_PORT = 4000; // Port for the WebSocket server

let protobufRoot = null;
let defaultClient = UpstoxClient.ApiClient.instance;
let apiVersion = '2.0';
let OAUTH2 = defaultClient.authentications['OAUTH2'];
OAUTH2.accessToken = process.env.AccessToken; // Your Upstox API token

let wsClient = null; // WebSocket instance for Upstox
let webSocketServer = null; // WebSocket server instance
let latestData = {}; // Latest data to be sent to WebSocket clients

// Initialize Protobuf
const initProtobuf = async () => {
  protobufRoot = await protobuf.load(__dirname + '/MarketDataFeed.proto');
  console.log('Protobuf part initialization complete');
};

// Function to authorize and get WebSocket URL
const getMarketFeedUrl = async () => {
  return new Promise((resolve, reject) => {
    let apiInstance = new UpstoxClient.WebsocketApi();
    apiInstance.getMarketDataFeedAuthorize(apiVersion, (error, data) => {
      if (error) {
        reject(error);
      } else {
        resolve(data.data.authorizedRedirectUri);
      }
    });
  });
};

// Function to connect to WebSocket and listen for data
const connectWebSocket = async () => {
  try {
    const wsUrl = await getMarketFeedUrl(); // Get WebSocket URL
    wsClient = new WebSocket(wsUrl, {
      headers: {
        'Api-Version': apiVersion,
        Authorization: 'Bearer ' + OAUTH2.accessToken,
      },
      followRedirects: true,
    });

    wsClient.on('open', () => {
      console.log('Connected to Upstox WebSocket');

      // Subscribe to the market data feed every second
      setInterval(() => {
        if (wsClient.readyState === WebSocket.OPEN) {
          const subscribeMessage = {
            guid: "someguid",
            method: "sub",
            data: {
              mode: "full",
              instrumentKeys: ["NSE_INDEX|Nifty Bank", "NSE_INDEX|Nifty 50"],
            },
          };
          wsClient.send(Buffer.from(JSON.stringify(subscribeMessage)));
        }
      }, 1000); // Send subscription message every 1 second
    });

    wsClient.on('message', (data) => {
      latestData = decodeProtobuf(data); // Decode and update the latest data
      // if (webSocketServer) {
      //   webSocketServer.clients.forEach(client => {
      //     if (client.readyState === WebSocket.OPEN) {
      //       // Send updated data
      //       // client.send(JSON.stringify({ timestamp: new Date(), data: latestData }));
      //     }
      //   });
      // }
    });

    wsClient.on('error', (error) => {
      console.error('Upstox WebSocket error:', error);
    });

    wsClient.on('close', () => {
      console.log('Upstox WebSocket connection closed');
    });
  } catch (error) {
    console.error('Error connecting to WebSocket:', error);
  }
};

// Function to decode Protobuf messages
const decodeProtobuf = (buffer) => {
  if (!protobufRoot) {
    console.warn('Protobuf part not initialized yet!');
    return null;
  }
  const FeedResponse = protobufRoot.lookupType('com.upstox.marketdatafeeder.rpc.proto.FeedResponse');
  return FeedResponse.decode(buffer);
};

// Function to set up WebSocket server
const setupWebSocketServer = () => {
  webSocketServer = new WebSocket.Server({ port: WS_PORT }); // WebSocket server port

  webSocketServer.on('connection', (socket) => {
    console.log('New WebSocket client connected');
    // Optionally send a welcome message or initial data to the new client
    // socket.send(JSON.stringify({ message: 'Welcome to the WebSocket server!' }));
  });

  // This interval broadcasts latest data to all connected WebSocket clients every second
  // setInterval(() => {
  //   if (webSocketServer) {
  //     webSocketServer.clients.forEach(client => {
  //       if (client.readyState === WebSocket.OPEN) {
  //         // Send updated data
  //         // client.send(JSON.stringify({ timestamp: new Date(), data: latestData }));
  //       }
  //     });
  //   }
  // }, 1000); // Send updates every 1 second
};

// Function to initialize WebSocket connection and server
const initializeWebSocket = async () => {
  await initProtobuf(); // Initialize protobuf
  setupWebSocketServer(); // Set up WebSocket server

  // Connect WebSocket initially
  await connectWebSocket();
};

// HTTP route to check the status and WebSocket URL
app.get('/', async (req, res) => {
  try {
    if (!webSocketServer) {
      await initializeWebSocket();
    }
    // Send the WebSocket server URL
    res.send({ message: 'WebSocket connection started successfully' });
  } catch (error) {
    console.error('An error occurred:', error);
    res.status(500).send('Failed to start WebSocket connection');
  }
});

app.get('/getLatestData', async (req, res) => {
  try {
    if (latestData != null){
      res.status(200).send({ message: 'Data fetched successfully', data: latestData, staus: 200 });
    }
    // Send the WebSocket server URL
  } catch (error) {
    console.error('An error occurred:', error);
    res.status(500).send('Failed to start WebSocket connection');
  }
});

// Start the HTTP server
app.listen(HTTP_PORT, async () => {
  await initializeWebSocket(); // Initialize WebSocket when the HTTP server starts
  console.log(`HTTP server running on http://localhost:${HTTP_PORT}`);
});