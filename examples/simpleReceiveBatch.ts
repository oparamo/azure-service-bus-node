import { Message, ReceiveMode, Namespace } from "../lib";
import * as dotenv from "dotenv";
dotenv.config();

const str = process.env["SERVICEBUS_CONNECTION_STRING"] || "";
const path = process.env["QUEUE_NAME"] || "";

console.log("str: ", str);
console.log("path: ", path);

async function main(): Promise<void> {
  const ns: Namespace = Namespace.createFromConnectionString(str);
  const client = ns.createQueueClient(path, { receiveMode: ReceiveMode.peekLock, maxConcurrentCalls: 1 });

  console.log("Created listener");

  // resolves when 3 messages have been received or 3 seconds have elapsed
  const messages = await client.receiveBatch(3, 3);
  const totalMessages = messages.length;

  console.log("Total batch received: ", totalMessages);

  for (let index = 0; index < totalMessages; index++) {
    const brokeredMessage: Message = messages[index];
    const messageBody = brokeredMessage.body ? brokeredMessage.body.toString() : null;

    console.log(`Message #${index} body: ${messageBody}`);

    brokeredMessage.complete();
  }

  return ns.close();
}

main().catch((err) => console.log("Error: ", err));
