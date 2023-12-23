const rascalBroker = require("rascal").BrokerAsPromised;

class RabbitMqBroker {
  static brokers = {};

  static async getBroker(config, brokerKey) {
    if (!this.brokers[brokerKey]) {
      try {
        console.log(`Creating a new broker instance for config: ${brokerKey}`);
        this.brokers[brokerKey] = await rascalBroker.create(config);
        return this.brokers[brokerKey];
      } catch (e) {
        console.error(`getBroker exception for config: ${brokerKey}`, e);
        return null;
      }
    } else {
      console.log(`returning existing object for config: ${brokerKey}`);
      return this.brokers[brokerKey];
    }
  }
}
module.exports = RabbitMqBroker;
