import { OnMessage, OnError, MessagingError, delay, Message, ReceiveMode, Namespace } from "../lib";
import * as dotenv from "dotenv";
dotenv.config();

const str = process.env["SERVICEBUS_CONNECTION_STRING"] || "";
const path = process.env["QUEUE_NAME"] || "";

console.log("str: ", str);
console.log("path: ", path);

async function main(): Promise<void> {
  const ns: Namespace = Namespace.createFromConnectionString(str);
  const client = ns.createQueueClient(path, { receiveMode: ReceiveMode.peekLock });

  console.log("Created listener");

  const onMessage: OnMessage = async (brokeredMessage: Message) => {
    const messageBody = brokeredMessage.body ? brokeredMessage.body.toString() : null;

    console.log("Message body: ", messageBody);

    brokeredMessage.complete();
  }

  const onError: OnError = (err: MessagingError | Error) => {
    console.log("Error consuming message: ", err);
  };

  client.receive(onMessage, onError, { autoComplete: true, maxConcurrentCalls: 1 });

  console.log("Listening for messages");

  // give the receiver some time to consume messages
  await delay(3000);

  return ns.close();
}

main().catch((err) => console.log("Error: ", err));
