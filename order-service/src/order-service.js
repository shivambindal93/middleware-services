const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const uuid = require("uuid");
const RabbitMqBroker = require("./broker");
const Rascal = require("rascal");
const fs = require("fs");
// RabbitMq config
const rabbitMqConfig1 = require("../rabbitmq-config/notification-service1.json");
const rabbitMqConfig2 = require("../rabbitmq-config/notification-service2.json");
const withDefaultConfig1 = Rascal.withDefaultConfig(rabbitMqConfig1);
const withDefaultConfig2 = Rascal.withDefaultConfig(rabbitMqConfig2);
const PROTO_PATH = "./protos/order.proto";
const JSON_FILE_PATH = "./assets/orders.json";

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});
const orderServiceProto = grpc.loadPackageDefinition(packageDefinition).order;

const orders = loadOrders();

function loadOrders() {
  try {
    const data = fs.readFileSync(JSON_FILE_PATH, "utf8");
    console.log("orders loaded successfully");
    return JSON.parse(data);
  } catch (error) {
    console.error("Error loading orders:", error);
    return [];
  }
}

function saveOrders() {
  try {
    const data = JSON.stringify(orders);
    fs.writeFileSync(JSON_FILE_PATH, data, "utf8");
    console.log("orders saved successfully");
  } catch (error) {
    console.error("Error saving orders:", error);
  }
}

const server = new grpc.Server();

const OrderEventType = Object.freeze({
  CREATE: "CREATE",
  UPDATE: "UPDATE"
});
server.addService(orderServiceProto.Order.service, {
  PlaceOrder: (call, callback) => {
    console.log("place order request", call.request);
    const { id, color, description, price } = call.request;
    const orderId = uuid.v4();
    const order = {
      orderId,
      id,
      color,
      description,
      price
    };
    orders.push(order);
    saveOrders();
    console.log("Order placed:", order);
    publishOrderEvent(OrderEventType.CREATE, {
      orderId,
      id,
      color,
      description,
      price
    });
    callback(null, { orderId, message: "Order placed successfully." });
  },

  UpdateOrder: (call, callback) => {
    console.log("update order request", call.request);
    const { id, color, description, price } = call.request;
    const existingOrder = orders.find((order) => order.id === id);

    if (existingOrder) {
      existingOrder.color = color;
      existingOrder.description = description;
      existingOrder.price = price;
      saveOrders();
      console.log("Order updated:", existingOrder);
      publishOrderEvent(OrderEventType.UPDATE, {
        id,
        color,
        description,
        price
      });
      callback(null, { orderId: id, message: "Order updated successfully." });
    } else {
      callback({ code: grpc.status.NOT_FOUND, details: "Order not found." });
    }
  }
});

const PORT = process.env.PORT || 50052;
server.bindAsync(`0.0.0.0:${PORT}`, grpc.ServerCredentials.createInsecure(), (err, port) => {
  if (err) {
    console.error("Error starting server:", err);
  } else {
    console.log(`Order Service running at http://0.0.0.0:${port}`);
    server.start();
  }
});

async function publishOrderEvent(eventType, eventData) {
  console.log("publishOrderEvent eventType ", eventType, " eventData ", eventData);
  const broker1 = await RabbitMqBroker.getBroker(withDefaultConfig1, "notificationService1");
  const broker2 = await RabbitMqBroker.getBroker(withDefaultConfig2, "notificationService2");
  try {
    let promises = [];
    if (eventType === OrderEventType.CREATE) {
      promises.push(await broker1.publish("fanout_pub_notification_service_1", eventData));
      promises.push(await broker2.publish("fanout_pub_notification_service_2", eventData));
    } else if (eventType === OrderEventType.UPDATE) {
      promises.push(await broker2.publish("topic_pub_notification_service_2", eventData));
    }
    await Promise.all(promises);
    console.log(`Event published: ${eventType}`);
  } catch (error) {
    console.error("Error publishing event:", error);
  }
}
