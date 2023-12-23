const RabbitMqBroker = require("./broker");
const Rascal = require("rascal");
// RabbitMq config
const rabbitMqConfig = require("../rabbitmq-config/notification-service1.json");
const withDefaultConfig = Rascal.withDefaultConfig(rabbitMqConfig);
const subscribe = async () => {
  const broker = await RabbitMqBroker.getBroker(withDefaultConfig);
  broker
    .subscribe("fanout_sub_notification_service_1")
    .then((subscription) => {
      console.log("fanout_sub");
      subscription
        .on("message", async (message, content, ackOrNack) => {
          try {
            console.log("messageId", message.properties.messageId);
            console.log("message headers", message.properties.headers);
            console.log("message received " + JSON.stringify(content));
            ackOrNack();
          } catch (e) {
            console.error("exception in message subscribe", e);
            ackOrNack(e, { strategy: "nack" });
          }
        })
        .on("error", console.error);
    })
    .catch((error) => {
      console.error(error);
    });
  return true;
};

(async () => {
  let error;
  do {
    error = null;
    await subscribe().catch((err) => {
      error = err;
      console.error(error);
    });
    await sleep(30000);
  } while (error);
})();

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
