const express = require('express');
const WebSocket = require('ws');
const protobuf = require('protobufjs');
const UpstoxClient = require('upstox-js-sdk');
require('dotenv').config();

const app = express();
const HTTP_PORT = 3000;

app.use(express.json());

let protobufRoot = null;
let wsClient = null;
let latestData = {};
let errorDetails = null;
let subscribedMessage = {};
let webSocketInitialized = false;

const defaultClient = UpstoxClient.ApiClient.instance;
const apiVersion = '2.0';
const OAUTH2 = defaultClient.authentications['OAUTH2'];
OAUTH2.accessToken = null;

// Initialize Protobuf
const initProtobuf = async () => {
  try {
    protobufRoot = await protobuf.load(`${__dirname}/MarketDataFeed.proto`);
    console.log('Protobuf initialized');
  } catch (error) {
    console.error('Failed to initialize Protobuf:', error);
  }
};

// Get WebSocket URL from Upstox API
const getMarketFeedUrl = () => {
  return new Promise((resolve, reject) => {
    const apiInstance = new UpstoxClient.WebsocketApi();
    apiInstance.getMarketDataFeedAuthorize(apiVersion, (error, data) => {
      if (error) {
        latestData = {};
        errorDetails = error;
        return reject(error);
      }
      errorDetails = null;
      resolve(data.data.authorizedRedirectUri);
    });
  });
};

// Connect to Upstox WebSocket and listen for data
const connectWebSocket = async () => {
  try {
    const wsUrl = await getMarketFeedUrl();

    wsClient = new WebSocket(wsUrl, {
      headers: {
        'Api-Version': apiVersion,
        Authorization: `Bearer ${OAUTH2.accessToken}`,
      },
      followRedirects: true,
    });

    wsClient.on('open', () => {
      errorDetails = null;
      console.log('Connected to Upstox WebSocket');
      webSocketInitialized = true;
      setInterval(subscribeToMarketData, 1);
    });

    wsClient.on('message', (data) => {
      errorDetails = null;
      latestData = decodeProtobuf(data);
      if(webSocketInitialized == true) {
        wsClient.close();
      }
      console.log('Received data from WebSocket:', latestData);
    });

    wsClient.on('error', (error) => {
      errorDetails = null;
      errorDetails = error;
      console.error('WebSocket error:', error);
    });

    wsClient.on('close', () => {
      console.log('WebSocket connection closed');
      webSocketInitialized = false;
    });
  } catch (error) {
    console.error('Failed to connect to WebSocket:', error);
  }
};

// Subscribe to market data feed
const subscribeToMarketData = () => {
  if (wsClient.readyState === WebSocket.OPEN) {
    wsClient.send(Buffer.from(JSON.stringify(subscribedMessage)));
    console.log('Subscribed to market data feed');
  }
};

// Decode Protobuf messages
const decodeProtobuf = (buffer) => {
  if (!protobufRoot) {
    console.warn('Protobuf not initialized');
    return null;
  }
  const FeedResponse = protobufRoot.lookupType('com.upstox.marketdatafeeder.rpc.proto.FeedResponse');
  return FeedResponse.decode(buffer);
};

// Initialize WebSocket connection and Protobuf
const initializeWebSocket = async () => {
  await initProtobuf();
  await connectWebSocket();
};

// HTTP route to start WebSocket connection
app.get('/', async (req, res) => {
  try {
    if (!webSocketInitialized) {
      await initializeWebSocket();
    }
    res.send({ message: 'WebSocket connection started' });
  } catch (error) {
    console.error('Failed to start WebSocket connection:', error);
    res.status(500).send('Error initializing WebSocket');
  }
});

// HTTP route to get the latest data
app.post('/getLatestData', async (req, res) => {
  try {
    subscribedMessage = req.body.subscribeMessage || {};
    OAUTH2.accessToken = req.body.accessToken || '';
    
    console.log('Subscribed Message:', subscribedMessage);
    
    if (!webSocketInitialized) {
      console.log('Initializing WebSocket...');
      await initializeWebSocket();
    }

    console.log("WHAT IS THIS::::", errorDetails == null);
    if (OAUTH2.accessToken && errorDetails == null) {
      console.log('Fetching data with access token...');
      setTimeout(() => {
        res.status(200).send({
          message: 'Data fetched successfully',
          data: latestData,
          accessToken: OAUTH2.accessToken,
          subscribedMessage,
          status: 200,
        });
      }, 300); // 300 milliseconds
    } else {
      res.status(400).send({
        message: 'Error Fetching Data',
        error: errorDetails,
        status: 400,
      });
    }
  } catch (error) {
    // Log the error stack to find where it came from
    console.error('Failed to fetch latest data:', error.stack || error);
    res.status(500).send({
      message: 'Error fetching data',
      error: error.message || error,
    });
  }
});


// Start the HTTP server
app.listen(HTTP_PORT, () => {
  console.log(`HTTP server running on http://localhost:${HTTP_PORT}`);
});