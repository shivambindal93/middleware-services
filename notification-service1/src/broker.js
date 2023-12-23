const rascalBroker = require("rascal").BrokerAsPromised;

class RabbitMqBroker {
  broker;

  static async getBroker(config) {
    if (!this.broker) {
      try {
        console.log("creating new object");
        this.broker = await rascalBroker.create(config);
        return this.broker;
      } catch (e) {
        console.error("getBroker exception:", e);
        return null;
      }
    } else {
      console.log("returning existing object");
      return this.broker;
    }
  }
}
module.exports = RabbitMqBroker;
