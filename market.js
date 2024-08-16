const express = require('express');
const WebSocket = require('ws');
const protobuf = require('protobufjs');
const UpstoxClient = require('upstox-js-sdk');
require('dotenv').config();


const app = express();
const HTTP_PORT = 3000; // Port for the HTTP server

app.use(express.json());

let protobufRoot = null;
let defaultClient = UpstoxClient.ApiClient.instance;
let apiVersion = '2.0';
let OAUTH2 = defaultClient.authentications['OAUTH2'];
OAUTH2.accessToken = ''; // Your Upstox API token

let wsClient = null; // WebSocket instance for Upstox
let webSocketServer = false; // WebSocket server instance
let latestData = {}; // Latest data to be sent to WebSocket clients
let subscribedMessage = {}; // Latest data to be sent to WebSocket clients

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

        if (wsClient.readyState === WebSocket.OPEN) {
          // const subscribeMessage = {
          //   guid: "someguid",
          //   method: "sub",
          //   data: {
          //     mode: "full",
          //     instrumentKeys: ["NSE_INDEX|Nifty Bank", "NSE_INDEX|Nifty 50"],
          //   },
          // };
          webSocketServer =true;
          wsClient.send(Buffer.from(JSON.stringify(subscribedMessage)));
          console.log("Senttttttttt");
        }
    });

    wsClient.on('message', (data) => {
      console.log(decodeProtobuf(data));
      console.log("Data from WebSocket");
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

// Function to initialize WebSocket connection and server
const initializeWebSocket = async () => {
  await initProtobuf(); // Initialize protobuf
  // Connect WebSocket initially
  await connectWebSocket();
};

// HTTP route to check the status and WebSocket URL
app.get('/', async (req, res) => {
  try {
    // if (!webSocketServer) {
    //   await initializeWebSocket();
    // }
    // Send the WebSocket server URL
    res.send({ message: 'Server Started' });
  } catch (error) {
    console.error('An error occurred:', error);
    res.status(500).send('Failed to start WebSocket connection');
  }
});

app.post('/getLatestData', async (req, res) => {
  try {
    // Print the incoming request body for debugging
    console.log('Request Body:', req.body);

    // Safely extract parameters from the request body
    if (req.body) {
      subscribedMessage = req.body.subscribeMessage || {};
      OAUTH2.accessToken = req.body.accessToken || '';
    } else {
      throw new Error('Request body is undefined');
    }

    // Print extracted values for debugging
    console.log('Extracted subscribedMessage:', subscribedMessage);
    console.log('Extracted accessToken:', OAUTH2.accessToken);

    // Check and initialize WebSocket server if necessary
    if (webSocketServer == false) {
    await initializeWebSocket();
    }

    // Check if latestData is not an empty object
    if (OAUTH2.accessToken != '') {
      res.status(200).send({ message: 'Data fetched successfully', data: latestData, accesToken: OAUTH2.accessToken, subscribedMessage: subscribedMessage, status: 200 });
    } else if (subscribedMessage == null || subscribedMessage == {}) {
      res.status(400).send({ message: 'Please send correct parameters of subscribedMessage', status: 400 });
    } else if (OAUTH2.accessToken == '') {
      res.status(400).send({ message: 'Access Token Not Provided', accesToken: OAUTH2.accessToken, subscribedMessage: subscribedMessage, status: 400 });
    } else {
      res.status(400).send({ message: 'Please contact admin', status: 400 });
    }
  } catch (error) {
    console.error('An error occurred:', error.message);
    res.status(500).send('Failed to fetch latest data');
  }
});

// Start the HTTP server
app.listen(HTTP_PORT, async () => {
  // await initializeWebSocket(); // Initialize WebSocket when the HTTP server starts
  console.log(`HTTP server running on http://localhost:${HTTP_PORT}`);
});