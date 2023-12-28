require("dotenv").config();
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const PROTO_PATH = "./protos/order.proto";

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});
const productServiceProto = grpc.loadPackageDefinition(packageDefinition).order;

const orderServiceClient = new productServiceProto.Order(
  process.env.ORDER_SERVICE_URL || "localhost:50052",
  grpc.credentials.createInsecure()
);

const server = new grpc.Server();

server.addService(productServiceProto.Order.service, {
  PlaceOrder: (call, callback) => {
    console.log("place order request", call.request);
    const products = call.request.products;
    orderServiceClient.PlaceOrder({ products }, (error, response) => {
      if (error) {
        console.error("Error placing order:", error);
        callback({
          code: grpc.status.INTERNAL,
          details: "Internal Server Error"
        });
      } else {
        console.log("place order response", response);
        callback(null, response);
      }
    });
  },
  UpdateOrder: (call, callback) => {
    console.log("update order request", call.request);
    const products = call.request.products;
    const orderId = call.request.orderId;
    orderServiceClient.UpdateOrder({ products, orderId }, (error, response) => {
      if (error) {
        console.error("Error updating order:", error);
        callback({
          code: grpc.status.INTERNAL,
          details: "Internal Server Error"
        });
      } else {
        console.log("update order response", response);
        callback(null, response);
      }
    });
  }
});

const PORT = process.env.PORT || 50051;
server.bindAsync(`0.0.0.0:${PORT}`, grpc.ServerCredentials.createInsecure(), (err, port) => {
  if (err) {
    console.error("Error starting server:", err);
  } else {
    console.log(`Product Service running at http://0.0.0.0:${port}`);
    server.start();
  }
});
